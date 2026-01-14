import logging
from fastapi import FastAPI, Response, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, Gauge

from pg_replica.database import (
    get_pipeline_summary, 
    get_resource_projections, 
    save_table_config,
    get_latest_table_config
)
from pg_replica.config import settings, SearchPipeline
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

from contextlib import asynccontextmanager
from pg_replica.database import init_pools, close_pools

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize DB pools on startup
    try:
        await init_pools(settings)
    except Exception as e:
        logger.error(f"Failed to initialize DB pools: {e}")
    yield
    # Clean up on shutdown
    await close_pools()

# FastAPI App
app = FastAPI(title="Search Replica Observability", lifespan=lifespan)


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
                "search_profile": v.serve.profiles.get(v.serve.default_profile, {}).mode if v.serve.default_profile in v.serve.profiles else "unknown",
                "model": v.pipeline.embedding.model,
                "version_id": v.get_version_id(),
                "generation": getattr(v, "_generation", 0)
            } for k, v in settings.pipelines.items()
        }
    }


@app.post("/control-plane/config/{target_name}")
async def update_config(target_name: str, config: SearchPipeline):
    """
    Apply a new configuration for a table.
    Runs admission validation (Dry Run) before persisting.
    """
    # If target doesn't exist, we allow creating it.
    # if target_name not in settings.pipelines:
    #    raise HTTPException(status_code=404, detail=f"Target {target_name} not found in current settings")
    pass

    # 1. Admission Control / Dry Run
    inspector = Inspector(settings)
    source_state = await inspector.get_source_state()
    sink_state = await inspector.get_sink_state()
    
    # Temporarily override to see if it plans correctly
    orig_config = settings.pipelines.get(target_name)
    settings.pipelines[target_name] = config
    
    planner = Planner(settings)
    try:
        actions = planner.plan(source_state, sink_state)
        # If planning succeeds, we consider it valid for now
    except Exception as e:
        if orig_config:
            settings.pipelines[target_name] = orig_config
        else:
            settings.pipelines.pop(target_name, None)
        raise HTTPException(status_code=400, detail=f"Configuration rejected by Planner: {e}")
    finally:
        # Restore original config for the main process loop
        if orig_config:
            settings.pipelines[target_name] = orig_config
        else:
            settings.pipelines.pop(target_name, None)

    # 2. Persist
    generation = await save_table_config(settings, target_name, config)
    
    # 3. Update In-Memory State (Critical for subsequent reads like promote)
    settings.pipelines[target_name] = config

    return {
        "status": "accepted",
        "target_name": target_name,
        "generation": generation,
        "config_hash": config.get_config_hash(),
        "actions_planned": len(actions)
    }


@app.post("/control-plane/dry-run/{target_name}")
async def dry_run(target_name: str, config: SearchPipeline = None):
    """
    Preview actions and resource projections for a proposed configuration.
    If no config provided, uses the latest one from DB or Settings.
    """
    if target_name not in settings.pipelines and not config:
        raise HTTPException(status_code=404, detail=f"Target {target_name} not found and no config provided")

    target_config = config or settings.pipelines[target_name]
    
    inspector = Inspector(settings)
    try:
        source_state = await inspector.get_source_state()
        sink_state = await inspector.get_sink_state()
    except Exception as e:
        logger.warning(f"DB Inspection failed: {e}. Proceeding with empty state (Offline Mode).")
        source_state = {"tables": {}, "publications": {}}
        sink_state = {"tables": {}, "vectorizers": {}}

    inspector = Inspector(settings) # Re-init might be needed if stateful, but it's not.
    # Actually we just needed the states.

    # Override
    orig_config = settings.pipelines.get(target_name)
    settings.pipelines[target_name] = target_config
    
    planner = Planner(settings)
    actions = planner.plan(source_state, sink_state)
    
    # Projections
    try:
        projections = await get_resource_projections(settings, target_config)
    except Exception:
        projections = {}
    
    # Restore
    if orig_config:
        settings.pipelines[target_name] = orig_config
    else:
        # If it was new, remove it to clean up
        settings.pipelines.pop(target_name, None)
    
    return {
        "target_name": target_name,
        "actions": [a.description for a in actions],
        "projections": projections
    }


@app.post("/control-plane/promote/{target_name}/{branch_name}")
async def promote_branch(target_name: str, branch_name: str):
    """
    SearchOps: Atomic Promotion of a Branch to Live.
    Clones branch config to parent and removes the branch.
    """
    # 1. Get current config
    current_config = settings.pipelines.get(target_name)
    if not current_config:
        # Try to fetch from DB if not in memory
        db_config_row = await get_latest_table_config(settings, target_name)
        if not db_config_row:
             raise HTTPException(status_code=404, detail=f"Pipeline {target_name} not found")
        current_config = SearchPipeline.model_validate(db_config_row["config_json"])

    # 2. Find the branch
    branch = next((b for b in current_config.storage.branches if b.name == branch_name), None)
    if not branch:
         raise HTTPException(status_code=404, detail=f"Branch '{branch_name}' not found for pipeline '{target_name}'")

    # 3. Create promoted config
    promoted_config = current_config.model_copy(deep=True)
    promoted_config.pipeline = branch.pipeline.model_copy(deep=True)
    
    # 4. Remove the branch from storage
    promoted_config.storage.branches = [b for b in promoted_config.storage.branches if b.name != branch_name]

    # 5. Admission Control (Dry Run)
    inspector = Inspector(settings)
    source_state = await inspector.get_source_state()
    sink_state = await inspector.get_sink_state()
    
    planner = Planner(settings)
    try:
        # Temporarily inject to plan
        settings.pipelines[target_name] = promoted_config
        actions = planner.plan(source_state, sink_state)
    except Exception as e:
        settings.pipelines[target_name] = current_config # Restore
        raise HTTPException(status_code=400, detail=f"Promotion rejected by Planner: {e}")
    finally:
        settings.pipelines[target_name] = current_config # Restore

    # 6. Persist
    generation = await save_table_config(settings, target_name, promoted_config)
    
    # 7. Update In-Memory State
    settings.pipelines[target_name] = promoted_config
    
    return {
        "status": "accepted",
        "target_name": target_name,
        "promoted_branch": branch_name,
        "generation": generation,
        "actions_planned": len(actions),
        "config": promoted_config.model_dump()
    }


@app.post("/control-plane/settings")
async def update_settings(updates: dict):
    """
    Update global settings (e.g. max_slot_wal_keep_size_mb).
    """
    try:
        # Update properites on the existing settings object
        for k, v in updates.items():
            if hasattr(settings, k) and k != "pipelines": # pipelines handled separately
                 setattr(settings, k, v)
        
        logger.info(f"Updated global settings: {updates.keys()}")
        return {"status": "updated", "updated_keys": list(updates.keys())}
    except Exception as e:
        logger.error(f"Failed to update settings: {e}")
        raise HTTPException(status_code=400, detail=str(e))


def update_replication_lag(table_name: str, lag_mb: float):
    """Update the replication lag metric."""
    REPLICATION_LAG_MB.labels(table=table_name).set(lag_mb)


def update_pgai_pending(table_name: str, pending_count: int):
    """Update the pgai pending items metric for a specific table."""
    PGAI_PENDING_ITEMS.labels(table=table_name).set(pending_count)

