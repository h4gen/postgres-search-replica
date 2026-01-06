from prometheus_client import Gauge

# Prometheus Gauges
REPLICATION_LAG_MB = Gauge(
    "replication_lag_mb", "Current replication lag in megabytes", ["table"]
)
PGAI_PENDING_ITEMS = Gauge(
    "pgai_pending_items", "Number of items pending in pgai vectorizer", ["table"]
)

def update_replication_lag(table_name: str, lag_mb: float):
    """Update Prometheus gauge for replication lag."""
    REPLICATION_LAG_MB.labels(table=table_name).set(lag_mb)

def update_pgai_pending_items(table_name: str, pending: int):
    """Update Prometheus gauge for pgai pending items."""
    PGAI_PENDING_ITEMS.labels(table=table_name).set(pending)
