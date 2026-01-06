import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock
from pg_replica.observability import app
from pg_replica.config import TableConfig

client = TestClient(app)

@pytest.fixture
def mock_replica():
    """Properly mock PGSearchReplica as an async context manager."""
    with patch("pg_replica.observability.PGSearchReplica") as m_cls, \
         patch("pg_replica.observability.get_source_column_types", new_callable=AsyncMock) as m_get_schema:
        
        # Setup the mock instance
        m_instance = MagicMock()
        m_cls.return_value = m_instance
        
        # Async context manager support
        m_instance.__aenter__ = AsyncMock(return_value=m_instance)
        m_instance.__aexit__ = AsyncMock(return_value=None)
        
        # Search method
        m_instance.search = AsyncMock()
        
        yield m_cls, m_instance, m_get_schema

def test_get_schema(mock_replica):
    m_cls, m_instance, m_get_schema = mock_replica
    m_get_schema.return_value = {"id": "integer", "title": "text"}
    
    response = client.get("/control-plane/schema/test_table")
    assert response.status_code == 200
    assert response.json()["columns"]["id"] == "integer"

@pytest.fixture
def mock_planner_dependencies():
    with patch("pg_replica.observability.Planner") as m_planner, \
         patch("pg_replica.observability.Inspector") as m_inspector, \
         patch("pg_replica.observability.get_resource_projections", new_callable=AsyncMock) as m_projections, \
         patch("pg_replica.observability.save_table_config", new_callable=AsyncMock) as m_save:
        
        # Inspector mocks
        inspector_instance = MagicMock()
        m_inspector.return_value = inspector_instance
        inspector_instance.get_source_state = AsyncMock(return_value={"tables": ["test_table"]})
        inspector_instance.get_sink_state = AsyncMock(return_value={})

        # Planner mocks
        planner_instance = MagicMock()
        m_planner.return_value = planner_instance
        action_mock = MagicMock()
        action_mock.description = "Create subscription"
        planner_instance.plan.return_value = [action_mock]

        # Projections mock
        m_projections.return_value = {"estimated_ram_mb": 100}

        # Save mock
        m_save.return_value = 1  # generation 1

        yield m_planner, m_inspector, m_projections, m_save

def test_dry_run_with_config(mock_planner_dependencies):
    payload = {
        "source_table": "test_table",
        "publication_columns": ["id", "content"],
        "embedding_model": "nomic-embed-text",
        "search_profile": "vector"
    }
    response = client.post("/control-plane/dry-run/test_table", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["target_name"] == "test_table"
    assert "Create subscription" in data["actions"]
    assert data["projections"]["estimated_ram_mb"] == 100

def test_dry_run_no_config_fallback(mock_planner_dependencies):
    # This specifically tests the fix where we handle targets not yet in settings
    with patch("pg_replica.observability.get_latest_table_config", new_callable=AsyncMock) as m_get_latest:
        m_get_latest.return_value = TableConfig(source_table="test_table")
        
        response = client.post("/control-plane/dry-run/test_table")
        
        # Should succeed by falling back to skeleton/history
        assert response.status_code == 200
        data = response.json()
        assert data["target_name"] == "test_table"

def test_update_config(mock_planner_dependencies):
    payload = {
        "source_table": "test_table",
        "publication_columns": ["id", "content"],
        "embedding_model": "nomic-embed-text",
        "search_profile": "vector"
    }
    response = client.post("/control-plane/config/test_table", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "accepted"
    assert data["generation"] == 1
