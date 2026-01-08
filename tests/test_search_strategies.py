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
    
    # Cleanup for this specific test
    await robust_slot_cleanup("sub_strat")

    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": internal_source_url}):
        # 1. Setup Source
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
        # Ensure publication exists for the new table
        try:
            await source_conn.execute("DROP PUBLICATION IF EXISTS pub_strat_products")
            await source_conn.execute("CREATE PUBLICATION pub_strat_products FOR TABLE strat_products")
        except Exception: pass

        # 2. Cleanup Sink Specifics
        await sink_conn.execute("DROP TABLE IF EXISTS strat_products CASCADE")
        await sink_conn.execute("DELETE FROM _replica_state WHERE key = 'sub_strat'")
        

        # Seed Source (Pre-seed to ensure snapshot picks it up)
        product_name = "Strategy Watch"
        logger.info(f"Connecting to source for seeding...")
        await source_conn.execute("INSERT INTO strat_products (name, description) VALUES (%s, %s)", (product_name, "A high-tech watch for strategic planning."))
        
        # Verify seeding
        async with source_conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM strat_products")
            count = (await cur.fetchone())[0]
            logger.info(f"Seeded source strat_products with {count} rows")

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

            # 2. Wait for Qdrant Sync (custom check)
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

            # Check for Postgres View (should exist if wait_for_pgai_sync passed)
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
async def test_search_alias_promotion(clean_db, robust_slot_cleanup, internal_source_url, source_conn, sink_conn, qdrant_cleanup):
    """
    Verify that Qdrant Aliases are updated when a version is promoted in Postgres.
    """
    from unittest.mock import patch
    qdrant = qdrant_cleanup
    
    await robust_slot_cleanup("sub_alias")

    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": internal_source_url}):
        # 1. Start with Version 1
        async with connect(
            source_url=global_settings.source_url,
            sink_url=global_settings.resolved_sink_url,
            pipelines={
                "products": {
                    "ingest": {
                        "table": "products", # Using shared table name might conflict if not cleaned? clean_db handles it.
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
            await source_conn.execute("DELETE FROM products")
            await source_conn.execute("INSERT INTO products (name, description) VALUES ('V1 Product', 'Description')")
            
            # Force migration column if missing (legacy compat)
            try:
                await sink_conn.execute("ALTER TABLE _sink_mirror_registry ADD COLUMN IF NOT EXISTS promoted_version_id TEXT")
            except Exception: pass

            # Wait for promotion
            for i in range(30):
                try:
                    async with sink_conn.cursor() as cur:
                        await cur.execute("SELECT count(*) FROM products_search")
                        break
                except Exception: pass
                await asyncio.sleep(1)
            else:
                pytest.fail("Timed out waiting for Postgres view")
            
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

        # 2. Upgrade to Version 2 (Change formatting)
        # We need to restart the replica with new config
        # Ideally we'd use the reconciler directly, but `connect` is a high level wrapper
        
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
            # IMPORTANT: Re-use state so it sees it as an update, not a fresh start?
            # connect() creates a fresh Orchestrator. 
            # If the DB state is preserved (which it is, clean_db fixture ran at START of function), 
            # the new orchestrator will read metadata from _replica_config_history.
        ) as replica:
            # Wait for promotion to V2
            for _ in range(30):
                try:
                    async with sink_conn.cursor() as cur:
                        await cur.execute("SELECT view_definition FROM information_schema.views WHERE table_name = 'products_search'")
                        row = await cur.fetchone()
                        if row and v1_collection not in row[0]: 
                            break
                except Exception: pass
                await asyncio.sleep(1)
            else:
                 pytest.fail("Timed out waiting for Postgres V2 promotion")
            
            # Verify Qdrant Alias
            for _ in range(30):
                aliases = qdrant.get_aliases().aliases
                found = next((a for a in aliases if a.alias_name == "alias_test_products_production"), None)
                if found and found.collection_name != v1_collection:
                    break
                await asyncio.sleep(1)
                
            aliases = qdrant.get_aliases().aliases
            found = next((a for a in aliases if a.alias_name == "alias_test_products_production"), None)
            assert found.collection_name != v1_collection
            
