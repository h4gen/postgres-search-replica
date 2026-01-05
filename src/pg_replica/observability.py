import logging
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, Gauge

from pg_replica.database import get_pipeline_summary, get_resource_projections
from pg_replica.config import settings

logger = logging.getLogger(__name__)

# Prometheus Metrics
REPLICATION_LAG_MB = Gauge(
    "replication_lag_mb", "Current replication lag in megabytes", ["table"]
)
PGAI_PENDING_ITEMS = Gauge(
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
    for table_name, config in settings.tables.items():
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
                "version_id": v.get_version_id()
            } for k, v in settings.tables.items()
        }
    }


def update_replication_lag(table_name: str, lag_mb: float):
    """Update the replication lag metric."""
    REPLICATION_LAG_MB.labels(table=table_name).set(lag_mb)


def update_pgai_pending(table_name: str, pending_count: int):
    """Update the pgai pending items metric for a specific table."""
    PGAI_PENDING_ITEMS.labels(table=table_name).set(pending_count)

