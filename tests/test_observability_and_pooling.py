import pytest
import json
import logging
import io
from fastapi.testclient import TestClient
from pg_replica.observability import (
    app,
    update_replication_lag,
    update_pgai_pending,
)
from pg_replica.database import (
    init_pools,
    close_pools,
    get_source_conn,
    get_sink_conn,
)
from pg_replica.config import settings
from pythonjsonlogger.json import JsonFormatter

# --- Observability and Pooling Tests ---

client = TestClient(app)


def test_observability_health_endpoint():
    """Verify the /health endpoint returns 200 OK."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_observability_metrics_endpoint():
    """Verify the /metrics endpoint returns Prometheus format metrics."""
    # Seed some metrics
    update_replication_lag("test_table", 123.45)
    update_pgai_pending("test_table", 10)

    response = client.get("/metrics")
    assert response.status_code == 200
    content = response.text
    assert 'replication_lag_mb{table="test_table"} 123.45' in content
    assert 'pgai_pending_items{table="test_table"} 10.0' in content


# --- Connection Pool Tests ---


@pytest.mark.asyncio
async def test_connection_pool_lifecycle():
    """Verify that database connection pools can be initialized and closed."""
    # Use default settings from global_settings (already patched in conftest if needed)
    await init_pools(settings)
    try:
        source_conn_ctx = await get_source_conn()
        sink_conn_ctx = await get_sink_conn()

        async with source_conn_ctx as conn:
            assert not conn.closed

        async with sink_conn_ctx as conn:
            assert not conn.closed
    finally:
        await close_pools()


# --- Structured Logging Tests ---


def test_json_structured_logging_format():
    """Verify that the configured logger outputs valid JSON."""
    log_output = io.StringIO()
    handler = logging.StreamHandler(log_output)
    formatter = JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    handler.setFormatter(formatter)

    test_logger = logging.getLogger("test_observability_logger")
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.INFO)

    test_logger.info("Test message", extra={"custom_field": "custom_value"})

    output = log_output.getvalue().strip()
    log_data = json.loads(output)

    assert "asctime" in log_data
    assert log_data["levelname"] == "INFO"
    assert log_data["message"] == "Test message"
    assert log_data["custom_field"] == "custom_value"
    assert log_data["name"] == "test_observability_logger"

