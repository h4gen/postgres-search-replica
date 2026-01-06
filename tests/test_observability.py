import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock
from pg_replica.observability import app
from pg_replica.config import TableConfig, Settings

client = TestClient(app)

@pytest.fixture
def mock_settings():
    s = Settings(source_url="postgresql://user:pass@localhost:5432/db", sink_url="local")
    s.tables = {
        "test_table": TableConfig(
            source_table="test_table",
            publication_columns=["id", "content"],
            embedding_model="test-model"
        )
    }
    return s

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_metrics():
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "replication_lag_mb" in response.text

@patch("pg_replica.observability.get_pipeline_summary")
@patch("pg_replica.observability.get_sink_conn")
@patch("pg_replica.observability.get_resource_projections")
@patch("pg_replica.observability.settings")
def test_control_plane_summary(mock_settings_val, mock_projections, mock_sink_conn, mock_summary):
    # Setup mocks
    mock_summary.return_value = {"source": {"is_connected": True}, "vectorizers": [], "mirrors": []}
    mock_projections.return_value = {"estimated_ram_mb": 100}
    
    # Mock DB response for persisted configs
    mock_cursor = AsyncMock()
    mock_cursor.fetchall.return_value = [
        {
            "target_name": "db_table",
            "config_json": {
                "source_table": "db_table",
                "publication_columns": ["id"],
                "embedding_model": "db-model"
            },
            "generation": 1
        }
    ]
    
    # In psycopg3, cursor() is a sync call returning an AsyncCursor
    mock_conn = MagicMock() 
    mock_conn.cursor.return_value.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    
    mock_sink_conn.return_value = mock_conn
    # Trigger endpoint
    response = client.get("/control-plane/summary")
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "db_table" in data["config_summaries"]
    assert data["config_summaries"]["db_table"]["model"] == "db-model"

@patch("pg_replica.observability.Inspector")
@patch("pg_replica.observability.Planner")
@patch("pg_replica.observability.get_resource_projections")
@patch("pg_replica.observability.settings")
def test_dry_run(mock_settings_val, mock_projections, mock_planner, mock_inspector):
    # Setup mocks
    mock_settings_val.tables = {"test_table": MagicMock(spec=TableConfig)}
    mock_inspector.return_value.get_source_state = AsyncMock(return_value={})
    mock_inspector.return_value.get_sink_state = AsyncMock(return_value={})
    
    mock_action = MagicMock()
    mock_action.description = "Test Action"
    mock_planner.return_value.plan.return_value = [mock_action]
    mock_projections.return_value = {"estimated_ram_mb": 50}
    
    # Post with config
    config_payload = {
        "source_table": "test_table",
        "publication_columns": ["id", "val"],
        "embedding_model": "new-model"
    }
    
    response = client.post("/control-plane/dry-run/test_table", json=config_payload)
    
    assert response.status_code == 200
    data = response.json()
    assert "Test Action" in data["actions"]
    assert data["projections"]["estimated_ram_mb"] == 50
