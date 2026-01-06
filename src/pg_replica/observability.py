import logging
from typing import List, Optional
from fastapi import FastAPI, Response, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from psycopg.rows import dict_row
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, Gauge
import os

from pg_replica.database import (
    get_pipeline_summary, 
    get_resource_projections, 
    save_table_config,
    get_latest_table_config,
    get_sink_conn,
    get_source_column_types
)
from pg_replica.config import Settings, TableConfig, settings
from pg_replica.reconciler import Planner, Inspector
from pg_replica.client import PGSearchReplica
from pg_replica.metrics import REPLICATION_LAG_MB, PGAI_PENDING_ITEMS

logger = logging.getLogger(__name__)

# Models for the Control Plane

# Models for the Control Plane
class SearchRequest(BaseModel):
    query: str
    limit: int = 5
    version_id: Optional[str] = None
    engine: Optional[str] = None

class SearchResult(BaseModel):
    id: str
    content: str
    distance: float
    metadata: Optional[dict] = None

# FastAPI App
app = FastAPI(title="Search Replica Observability")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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

    # 1. Load latest configs from DB to ensure UI reflects persistent state
    # This covers targets registered via CLI/API that Reconciler hasn't picked up yet
    db_configs = {}
    async with await get_sink_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            try:
                await cur.execute(
                    """
                    SELECT DISTINCT ON (target_name) target_name, config_json, generation
                    FROM _replica_config_history
                    ORDER BY target_name, generation DESC
                    """
                )
                for row in await cur.fetchall():
                    target_name = row["target_name"]
                    config_data = row["config_json"]
                    try:
                        # Ensure required fields exist in older persisted configs
                        if "source_table" not in config_data:
                            config_data["source_table"] = target_name
                        
                        cfg = TableConfig(**config_data)
                        setattr(cfg, "_generation", row["generation"]) 
                        db_configs[target_name] = cfg
                    except Exception as e:
                        logger.error(f"Failed to parse persisted config for {target_name}: {e}")
                        continue
            except Exception:
                pass # Table might not exist yet

    # Merge with settings.tables (Settings takes precedence for ephemeral overrides if any)
    all_tables = {**db_configs, **settings.tables}

    # 2. Enrich with projections and real-time history
    projections = {}
    event_log = []
    
    async with await get_sink_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Fetch latest 20 events for the log
            try:
                await cur.execute(
                    """
                    SELECT target_name, generation, status, error_message, created_at 
                    FROM _replica_config_history 
                    ORDER BY created_at DESC LIMIT 20
                    """
                )
                event_log = await cur.fetchall()
            except Exception: pass

    for table_name, config in all_tables.items():
        try:
            projections[table_name] = await get_resource_projections(settings, config)
            # Find matching vectorizer status in pipeline summary to add pending context
            v_status = next((v for v in summary["vectorizers"] if v["source_table"] == config.source_table), None)
            if v_status:
                projections[table_name]["pending_items"] = v_status.get("pending_items", 0)
        except Exception as e:
            logger.warning(f"Could not calculate projections for {table_name}: {e}")
            projections[table_name] = {"error": str(e)}

    return {
        "status": "ok",
        "pipeline": summary,
        "projections": projections,
        "event_log": event_log,
        "config_summaries": {
            k: {
                "search_profile": v.search_profile,
                "model": v.embedding_model,
                "version_id": v.get_version_id(),
                "generation": getattr(v, "_generation", 0)
            } for k, v in all_tables.items()
        }
    }


@app.get("/control-plane/schema/{target_name}")
async def get_schema(target_name: str):
    """Fetch column names and types for a source table."""
    if target_name not in settings.tables:
        # If it's not in settings, try to create a skeleton config for detection
        config = TableConfig(source_table=target_name)
    else:
        config = settings.tables[target_name]
    
    try:
        col_types = await get_source_column_types(settings, config)
        return {"target_name": target_name, "columns": col_types}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to fetch schema: {e}")



@app.post("/control-plane/config/{target_name}")
async def update_config(target_name: str, config: TableConfig):
    """
    Apply a new configuration for a table.
    Runs admission validation (Dry Run) before persisting.
    """
    # 1. Admission Control / Dry Run
    inspector = Inspector(settings)
    source_state = await inspector.get_source_state()
    sink_state = await inspector.get_sink_state()
    
    # Temporarily override to see if it plans correctly
    orig_config = settings.tables.get(target_name)
    settings.tables[target_name] = config
    
    planner = Planner(settings)
    try:
        actions = planner.plan(source_state, sink_state)
        # If planning succeeds, we consider it valid for now
    except Exception as e:
        if orig_config:
            settings.tables[target_name] = orig_config
        else:
            del settings.tables[target_name]
        raise HTTPException(status_code=400, detail=f"Configuration rejected by Planner: {e}")
    finally:
        # Restore original config for the main process loop
        if orig_config:
            settings.tables[target_name] = orig_config
        else:
            del settings.tables[target_name]

    # 2. Persist
    generation = await save_table_config(settings, target_name, config)
    
    return {
        "status": "accepted",
        "target_name": target_name,
        "generation": generation,
        "config_hash": config.get_config_hash(),
        "actions_planned": len(actions)
    }


@app.post("/control-plane/dry-run/{target_name}")
async def dry_run(target_name: str, config: TableConfig = None):
    """
    Preview actions and resource projections for a proposed configuration.
    If no config provided, uses the latest one from DB or Settings.
    """
    target_config = config
    if target_config is None:
        if target_name in settings.tables:
            target_config = settings.tables[target_name]
        else:
            # Try to load from history or create skeleton
            latest_row = await get_latest_table_config(settings, target_name)
            if latest_row and "config_json" in latest_row:
                 # It returns a db row with config_json
                 try:
                     target_config = TableConfig(**latest_row["config_json"])
                 except Exception as e:
                     logger.warning(f"Failed to parse history config for {target_name}: {e}")
                     target_config = None
            
            if not target_config:
                target_config = TableConfig(source_table=target_name)

    inspector = Inspector(settings)
    source_state = await inspector.get_source_state()
    sink_state = await inspector.get_sink_state()
    
    # Override
    orig_config = settings.tables.get(target_name)
    settings.tables[target_name] = target_config
    
    planner = Planner(settings)
    actions = planner.plan(source_state, sink_state)
    
    # Projections
    projections = await get_resource_projections(settings, target_config)
    
    # Restore
    if orig_config:
        settings.tables[target_name] = orig_config
    else:
        del settings.tables[target_name]
    
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




