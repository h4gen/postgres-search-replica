import logging
from fastapi import FastAPI, Response, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, Gauge

from pg_replica.database import (
    get_pipeline_summary, 
    get_resource_projections, 
    save_table_config,
    get_latest_table_config
)
from pg_replica.config import settings, TableConfig
from pg_replica.reconciler import Planner, Inspector

logger = logging.getLogger(__name__)

# Prometheus Metrics
from prometheus_client import REGISTRY

def _get_or_create_gauge(name, documentation, labelnames):
    try:
        return Gauge(name, documentation, labelnames)
    except ValueError:
        # If it already exists, return the existing one from registry
        # This is a hack for test compatibility where modules are re-imported
        for collector in REGISTRY._collector_to_names.keys():
            if name in REGISTRY._collector_to_names[collector]:
                return collector
        raise

REPLICATION_LAG_MB = _get_or_create_gauge(
    "replication_lag_mb", "Current replication lag in megabytes", ["table"]
)
PGAI_PENDING_ITEMS = _get_or_create_gauge(
    "pgai_pending_items", "Number of items pending in pgai vectorizer", ["table"]
)

# FastAPI App
app = FastAPI(title="Search Replica Observability")


@app.get("/health")
async def health():
    """Liveness and readiness check."""
    # In a more complex setup, we could check DB pools here
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/control-plane/summary")
async def control_plane_summary():
    """Unified state object for the management UI."""
    summary = await get_pipeline_summary(settings)
    
    # Enrich with projections for each table
    projections = {}
    for table_name, config in settings.pipelines.items():
        try:
            projections[table_name] = await get_resource_projections(settings, config)
        except Exception as e:
            logger.warning(f"Could not calculate projections for {table_name}: {e}")
            projections[table_name] = {"error": str(e)}
            
    return {
        "status": "ok",
        "pipeline": summary,
        "projections": projections,
        "config_summaries": {
            k: {
                "search_profile": v.search_profile,
                "model": v.embedding_model,
                "version_id": v.get_version_id(),
                "generation": getattr(v, "_generation", 0)
            } for k, v in settings.pipelines.items()
        }
    }


@app.post("/control-plane/config/{target_name}")
async def update_config(target_name: str, config: TableConfig):
    """
    Apply a new configuration for a table.
    Runs admission validation (Dry Run) before persisting.
    """
    if target_name not in settings.pipelines:
        raise HTTPException(status_code=404, detail=f"Target {target_name} not found in current settings")

    # 1. Admission Control / Dry Run
    inspector = Inspector(settings)
    source_state = await inspector.get_source_state()
    sink_state = await inspector.get_sink_state()
    
    # Temporarily override to see if it plans correctly
    orig_config = settings.pipelines[target_name]
    settings.pipelines[target_name] = config
    
    planner = Planner(settings)
    try:
        actions = planner.plan(source_state, sink_state)
        # If planning succeeds, we consider it valid for now
    except Exception as e:
        settings.pipelines[target_name] = orig_config
        raise HTTPException(status_code=400, detail=f"Configuration rejected by Planner: {e}")
    finally:
        # Restore original config for the main process loop
        settings.pipelines[target_name] = orig_config

    # 2. Persist
    generation = await save_table_config(settings, target_name, config)
    
    return {
        "status": "accepted",
        "target_name": target_name,
        "generation": generation,
        "config_hash": config.get_config_hash(),
        "actions_planned": len(actions)
    }


@app.get("/control-plane/dry-run/{target_name}")
async def dry_run(target_name: str, config: TableConfig = None):
    """
    Preview actions and resource projections for a proposed configuration.
    If no config provided, uses the latest one from DB or Settings.
    """
    if target_name not in settings.pipelines:
        raise HTTPException(status_code=404, detail=f"Target {target_name} not found")

    target_config = config or settings.pipelines[target_name]
    
    inspector = Inspector(settings)
    source_state = await inspector.get_source_state()
    sink_state = await inspector.get_sink_state()
    
    # Override
    orig_config = settings.pipelines[target_name]
    settings.pipelines[target_name] = target_config
    
    planner = Planner(settings)
    actions = planner.plan(source_state, sink_state)
    
    # Projections
    projections = await get_resource_projections(settings, target_config)
    
    # Restore
    settings.pipelines[target_name] = orig_config
    
    return {
        "target_name": target_name,
        "actions": [a.description for a in actions],
        "projections": projections
    }


def update_replication_lag(table_name: str, lag_mb: float):
    """Update the replication lag metric."""
    REPLICATION_LAG_MB.labels(table=table_name).set(lag_mb)


def update_pgai_pending(table_name: str, pending_count: int):
    """Update the pgai pending items metric for a specific table."""
    PGAI_PENDING_ITEMS.labels(table=table_name).set(pending_count)

