import asyncio
import logging
import pytest
import uuid
from qdrant_client import QdrantClient
from pg_replica import connect, settings as global_settings
from pg_replica.database import get_sink_conn, dict_row, connect_db
import os

logger = logging.getLogger(__name__)

@pytest.fixture
def qdrant_cleanup():
    # Setup
    qdrant = QdrantClient("http://localhost:6333")
    try:
        # Pre-cleanup
        collections = qdrant.get_collections().collections
        for col in collections:
            if col.name.startswith("strat_test_") or col.name.startswith("alias_test_"):
                qdrant.delete_collection(col.name)
    except Exception: pass
    
    yield qdrant
    
    # Teardown
    try:
        collections = qdrant.get_collections().collections
        for col in collections:
            if col.name.startswith("strat_test_") or col.name.startswith("alias_test_"):
                qdrant.delete_collection(col.name)
    except Exception: pass


@pytest.mark.asyncio
async def test_search_strategies_postgres_vs_qdrant(clean_db, robust_slot_cleanup, internal_source_url, source_conn, sink_conn, qdrant_cleanup, wait_for_pgai_sync):
    """
    Verify that we can search using both Postgres and Qdrant engines
    and get consistent results.
    """
    from unittest.mock import patch
    
    qdrant = qdrant_cleanup # Unpack fixture
    
    # 1. Setup Source Schema
    await source_conn.execute("DROP TABLE IF EXISTS strat_products CASCADE")
    await source_conn.execute(
        """
        CREATE TABLE strat_products (
            id SERIAL PRIMARY KEY,
            name TEXT,
            description TEXT
        )
        """
    )
    await source_conn.execute("ALTER TABLE strat_products REPLICA IDENTITY DEFAULT")
    
    # 2. Seed Source (Pre-seed to ensure snapshot picks it up)
    product_name = "Strategy Watch"
    logger.info(f"Connecting to source for seeding...")
    await source_conn.execute("INSERT INTO strat_products (name, description) VALUES (%s, %s)", (product_name, "A high-tech watch for strategic planning."))
    
    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": internal_source_url}):
        # Configure replica with a mirror
        async with connect(
            source_url=global_settings.source_url,
            sink_url=global_settings.resolved_sink_url,
            pipelines={
                "strat_products": {
                    "ingest": {
                        "table": "strat_products",
                        "columns": ["id", "name", "description"],
                        "p_key": "id"
                    },
                    "pipeline": {
                        "template": "Title: $name\nContent: $chunk",
                        "content_column": "description",
                        "chunking": {
                            "strategy": "recursive_character"
                        },
                        "embedding": {
                             "provider": "ollama",
                             "model": "nomic-embed-text",
                             "dimension": 768
                        }
                    },
                    "storage": {
                        "postgres": { "profile": "vector" },
                        "mirrors": [
                            {
                                "id": "qdrant_meta",
                                "type": "qdrant",
                                "config": {
                                    "url": "http://localhost:6333",
                                    "prefix": "strat_test_"
                                }
                            }
                        ]
                    },
                    "active": True
                }
            },
            sync=True
        ) as replica:
            # Wait for Sync (Postgres + Qdrant)
            logger.info("Waiting for sync (Postgres + Qdrant)...")
            
            # 1. Wait for Postgres Sync (robustely)
            if not await wait_for_pgai_sync(replica.settings, "strat_products", expected_count=1):
                pytest.fail("Timed out waiting for Postgres pgai sync")

            # 2. Wait for Qdrant Sync (custom check for this mirror)
            found_in_qdrant = False
            for i in range(30):
                try:
                    col_name_prefix = "strat_test_strat_products_"  
                    collections = qdrant.get_collections().collections
                    target_cols = [c.name for c in collections if c.name.startswith(col_name_prefix)]
                    
                    if target_cols:
                        for c_name in target_cols:
                            points = qdrant.scroll(collection_name=c_name, limit=10)[0]
                            if points:
                                if any(product_name in (p.payload.get("content") or "") for p in points):
                                    found_in_qdrant = True
                                    break
                except Exception: pass
                
                if found_in_qdrant: break
                await asyncio.sleep(1)
            
            assert found_in_qdrant, "Qdrant sync timed out"

            # Check for Postgres View
            view_exists = False
            try:
                async with sink_conn.cursor() as cur:
                    await cur.execute("SELECT count(*) FROM strat_products_search")
                    if (await cur.fetchone())[0] > 0:
                        view_exists = True
            except Exception: pass
            assert view_exists, "Postgres search view not found or empty"

            # Test Postgres Search
            res_pg = await replica.search("high-tech watch")
            assert len(res_pg) > 0
            assert product_name in res_pg[0]["content"]
            
            # Test Qdrant Search
            res_qdrant = await replica.search("high-tech watch", engine="qdrant")
            assert len(res_qdrant) > 0
            assert product_name in res_qdrant[0]["content"]
            
            assert res_pg[0]["id"] == res_qdrant[0]["id"]

@pytest.mark.asyncio
async def test_search_alias_promotion(clean_db, robust_slot_cleanup, internal_source_url, source_conn, sink_conn, qdrant_cleanup, wait_for_pgai_sync):
    """
    Verify that Qdrant Aliases are updated when a version is promoted in Postgres.
    """
    from unittest.mock import patch
    qdrant = qdrant_cleanup
    
    # 1. Setup Source Schema
    await source_conn.execute("DROP TABLE IF EXISTS products CASCADE")
    await source_conn.execute(
        """
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name TEXT,
            description TEXT
        )
        """
    )
    await source_conn.execute("ALTER TABLE products REPLICA IDENTITY DEFAULT")

    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": internal_source_url}):
        # Phase 1: V1 Deployment
        logger.info("--- Phase 1: V1 Deployment ---")
        async with connect(
            source_url=global_settings.source_url,
            sink_url=global_settings.resolved_sink_url,
            pipelines={
                "products": {
                    "ingest": {
                        "table": "products",
                        "columns": ["id", "name", "description"],
                        "p_key": "id"
                    },
                    "pipeline": {
                        "template": "V1: $name $chunk",
                        "content_column": "description",
                        "chunking": {"strategy": "recursive_character"},
                        "embedding": {"provider": "ollama", "model": "nomic-embed-text", "dimension": 768}
                    },
                    "storage": {
                        "postgres": {"profile": "vector"},
                        "mirrors": [{
                            "id": "m1", 
                            "type": "qdrant", 
                            "config": {"url": "http://localhost:6333", "prefix": "alias_test_"}
                        }]
                    },
                    "active": True
                }
            },
            sync=True
        ) as replica:
            # Seed data
            await source_conn.execute("INSERT INTO products (name, description) VALUES ('V1 Product', 'Description')")
            
            # Wait for V1 Sync
            if not await wait_for_pgai_sync(replica.settings, "products", expected_count=1):
                pytest.fail("Timed out waiting for V1 Postgres sync")
            
            # Verify Qdrant Alias
            v1_collection = None
            for i in range(30):
                try:
                    aliases = qdrant.get_aliases().aliases
                    found = next((a for a in aliases if a.alias_name == "alias_test_products_production"), None)
                    if found:
                        v1_collection = found.collection_name
                        break
                except Exception: pass
                await asyncio.sleep(1)
            else:
                pytest.fail("Timed out waiting for V1 Qdrant Alias")
                
            assert "alias_test_products_" in v1_collection

        # Phase 2: V2 Deployment (Update)
        logger.info("--- Phase 2: V2 Deployment ---")
        async with connect(
            source_url=global_settings.source_url,
            sink_url=global_settings.resolved_sink_url,
            pipelines={
                "products": {
                    "ingest": {
                        "table": "products",
                        "columns": ["id", "name", "description"],
                        "p_key": "id"
                    },
                    "pipeline": {
                        "template": "V2: $name $chunk",
                        "content_column": "description", 
                        "chunking": {"strategy": "recursive_character"},
                        "embedding": {"provider": "ollama", "model": "nomic-embed-text", "dimension": 768}
                    },
                    "storage": {
                        "postgres": {"profile": "vector"},
                        "mirrors": [{
                            "id": "m1", 
                            "type": "qdrant", 
                            "config": {"url": "http://localhost:6333", "prefix": "alias_test_"}
                        }]
                    },
                    "active": True
                }
            },
            sync=True,
        ) as replica:
            # Wait for V2 Sync & Promotion
            if not await wait_for_pgai_sync(replica.settings, "products", expected_count=1):
                pytest.fail("Timed out waiting for V2 Postgres sync/promotion")
            
            # Verify Qdrant Alias swapped
            for i in range(30):
                try:
                    aliases = qdrant.get_aliases().aliases
                    found = next((a for a in aliases if a.alias_name == "alias_test_products_production"), None)
                    if found and found.collection_name != v1_collection:
                        break
                except Exception: pass
                await asyncio.sleep(1)
            else:
                pytest.fail("Timed out waiting for V2 Qdrant Alias swap")
                
            aliases = qdrant.get_aliases().aliases
            found = next((a for a in aliases if a.alias_name == "alias_test_products_production"), None)
            assert found.collection_name != v1_collection
            
