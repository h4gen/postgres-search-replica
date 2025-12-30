import logging
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, Gauge

logger = logging.getLogger(__name__)

# Prometheus Metrics
REPLICATION_LAG_MB = Gauge(
    "replication_lag_mb", "Current replication lag in megabytes"
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


def update_replication_lag(lag_mb: float):
    """Update the replication lag metric."""
    REPLICATION_LAG_MB.set(lag_mb)


def update_pgai_pending(table_name: str, pending_count: int):
    """Update the pgai pending items metric for a specific table."""
    PGAI_PENDING_ITEMS.labels(table=table_name).set(pending_count)

