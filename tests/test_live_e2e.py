import pytest
import httpx
from pg_replica.config import TableConfig

# Target the live service running in Docker
BASE_URL = "http://localhost:8000"

def test_live_dry_run_production_docs():
    """
    Verify the dry-run endpoint works for production_docs on the live server.
    This confirms the 404 fix is deployed and working end-to-end.
    """
    target = "production_docs"
    # We don't send a config, relying on the backend to find/create it (the fix)
    response = httpx.post(f"{BASE_URL}/control-plane/dry-run/{target}", timeout=10.0)
    
    assert response.status_code == 200, f"Dry run failed: {response.text}"
    data = response.json()
    assert data["target_name"] == target
    assert "projections" in data

def test_live_promote_production_docs():
    """
    Verify the config update (promotion) works for production_docs on the live server.
    """
    target = "production_docs"
    payload = {
        "source_table": target,
        "publication_columns": ["id", "title", "content"],
        "embedding_model": "nomic-embed-text",
        "search_profile": "vector"
    }
    
    response = httpx.post(f"{BASE_URL}/control-plane/config/{target}", json=payload, timeout=10.0)
    
    assert response.status_code == 200, f"Promotion failed: {response.text}"
    data = response.json()
    assert data["status"] == "accepted"
    assert data["target_name"] == target
