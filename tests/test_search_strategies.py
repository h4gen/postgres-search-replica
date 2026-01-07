import asyncio
import logging
import pytest
import uuid
from qdrant_client import QdrantClient
from pg_replica import connect
from pg_replica.database import get_sink_conn, dict_row, connect_db

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_search_strategies_postgres_vs_qdrant():
    """
    Verify that we can search using both Postgres and Qdrant engines
    and get consistent results.
    """
    import os
    
    # Use environment variables injected by Docker Compose, with defaults for local debugging
    source_url = os.environ.get("SOURCE_URL", "postgresql://postgres:password@source:5432/production_db")
    # For subscription, we always need the internal docker name if running in docker
    source_url_internal = os.environ.get("SUBSCRIPTION_SOURCE_URL", "postgresql://postgres:password@source:5432/production_db")
    # Sink URL should use 'local' to detect internal 54322 port, or configured value
    sink_url = os.environ.get("SINK_URL", "local")
    
    # Ensure Settings can see the internal URL via environment
    os.environ["SUBSCRIPTION_SOURCE_URL"] = source_url_internal

    # 0. Setup Source Table (Must exist before subscription)
    # 0. Setup Source Table (Must exist before subscription)
    async with await connect_db(source_url) as conn:
        await conn.execute("DROP TABLE IF EXISTS strat_products CASCADE")
        await conn.execute(
            """
            CREATE TABLE strat_products (
                id SERIAL PRIMARY KEY,
                name TEXT,
                description TEXT
            )
            """
        )
        await conn.execute("ALTER TABLE strat_products REPLICA IDENTITY DEFAULT")
        # Ensure publication exists for the new table
        try:
            await conn.execute("DROP PUBLICATION IF EXISTS pub_strat_products")
            await conn.execute("CREATE PUBLICATION pub_strat_products FOR TABLE strat_products")
        except Exception: pass
        await conn.commit()
    
    
    # 0.5 Pre-Cleanup (Before Replica Start to avoid config reloading race)
    qdrant = QdrantClient("http://localhost:6333")
    try:
        collections = qdrant.get_collections().collections
        for col in collections:
            if col.name.startswith("strat_test_") or col.name.startswith("alias_test_"):
                qdrant.delete_collection(col.name)
    except Exception: pass

    async with await connect_db(sink_url) as conn:
        logger.info("PRE-RUN CLEANUP: Starting...")
        # Force migration just in case
        await conn.execute("ALTER TABLE _sink_mirror_registry ADD COLUMN IF NOT EXISTS promoted_version_id TEXT")
        await conn.execute("TRUNCATE _sink_outbox CASCADE")
        await conn.execute("TRUNCATE _sink_mirror_registry CASCADE")
        
        # Robustly clear history if it exists
        try:
            # Check what's in there first
            async with conn.cursor() as cur:
                await cur.execute("SELECT target_name FROM _replica_config_history")
                rows = await cur.fetchall()
                logger.info(f"PRE-RUN CLEANUP: Found configs in history: {[r[0] for r in rows]}")

            await conn.execute("TRUNCATE _replica_config_history CASCADE")
            await conn.commit()
            logger.info("PRE-RUN CLEANUP: Truncated _replica_config_history and committed.")
            
            # Verify it's gone
            async with conn.cursor() as cur:
                await cur.execute("SELECT count(*) FROM _replica_config_history")
                count = (await cur.fetchone())[0]
                logger.info(f"PRE-RUN CLEANUP: Verify _replica_config_history count = {count}")
        except Exception as e:
            # Only ignore if table doesn't exist, otherwise RAISE
            if "UndefinedTable" not in str(e) and "does not exist" not in str(e):
                logger.error(f"PRE-RUN CLEANUP FAILED to truncate history: {e}")
                raise e
            logger.info("PRE-RUN CLEANUP: _replica_config_history does not exist (skipping)")
        
        # Drop ALL previous test artifacts and subscriptions
        # This prevents 'max_logical_replication_workers' from being exceeded by zombie tests
        try:
            async with conn.cursor() as cur:
                await cur.execute("SELECT subname FROM pg_subscription")
                subs = [r[0] for r in await cur.fetchall()]
                for sub in subs:
                    logger.info(f"PRE-RUN CLEANUP: Dropping zombie subscription {sub}")
                    try:
                        await cur.execute(f"ALTER SUBSCRIPTION {sub} DISABLE")
                        await cur.execute(f"ALTER SUBSCRIPTION {sub} SET (slot_name = NONE)")
                        await cur.execute(f"DROP SUBSCRIPTION {sub}")
                        logger.info(f"PRE-RUN CLEANUP: Dropped {sub}")
                    except Exception as e:
                        logger.warning(f"PRE-RUN CLEANUP: Failed to drop {sub}: {e}")
        except Exception as e:
             logger.warning(f"PRE-RUN CLEANUP: Failed to list subscriptions: {e}")

        await conn.execute("DROP TABLE IF EXISTS strat_products CASCADE")
        await conn.execute("DROP TABLE IF EXISTS strat_products_search CASCADE")
        await conn.commit()

    # Configure replica with a mirror
    async with connect(
        source_url=source_url,
        sink_url=sink_url,
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
        # 1. Seed Source
        # unique_id = str(uuid.uuid4())[:8] # Not needed with fixed product name
        product_name = "Strategy Watch 73b8704c"
        logger.info(f"Connecting to source for seeding... {source_url}")
        async with await connect_db(source_url) as conn:
            await conn.execute("SELECT 1")
            logger.info("Source connection successful.")
            await conn.execute("DELETE FROM strat_products")
            await conn.execute(
                "INSERT INTO strat_products (name, description) VALUES (%s, %s)",
                (product_name, "A high-tech watch for strategic planning.")
            )
            await conn.commit()
            logger.info("Seeded data successfully.")

        # 2. Wait for Sync (Postgres + Qdrant)
        # We need to verify that BOTH the PG View is created AND Qdrant has data
        found_in_qdrant = False
        view_exists = False
        
        for i in range(30):
            status = await replica.get_status()
            pending = sum(v.get("pending_items", 0) for v in status.get("vectorizers", []))
            
            # Check Qdrant
            try:
                col_name_prefix = "strat_test_strat_products_"  
                collections = qdrant.get_collections().collections
                target_cols = [c.name for c in collections if c.name.startswith(col_name_prefix)]
                
                if target_cols:
                    for c_name in target_cols:
                        points = qdrant.scroll(collection_name=c_name, limit=10)[0]
                        if points:
                            logger.info(f"Checking Qdrant collection {c_name}. Found {len(points)} points.")
                            if any(product_name in (p.payload.get("content") or "") for p in points):
                                found_in_qdrant = True
                                break
            except Exception as e:
                logger.warning(f"Qdrant check failed: {e}")

            # Check for Postgres View
            try:
                async with await get_sink_conn() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT table_name FROM information_schema.views WHERE table_schema = 'public'")
                        views = [row[0] for row in await cur.fetchall()]
                        if "strat_products_search" in views:
                            await cur.execute("SELECT count(*) FROM strat_products_search")
                            row = await cur.fetchone()
                            if row and row[0] > 0:
                                view_exists = True
            except Exception as e:
                logger.warning(f"View check failed: {e}")

            logger.info(f"Wait status: pending={pending}, qdrant={found_in_qdrant}, view={view_exists}")
            if pending == 0 and found_in_qdrant and view_exists:
                break
            await asyncio.sleep(2)
        else:
            pytest.fail(f"Timed out waiting for sync. State: qdrant={found_in_qdrant}, view={view_exists}")

        # 3. Test Postgres Search
        res_pg = await replica.search("high-tech watch")
        # Verify content
        assert len(res_pg) > 0
        assert product_name in res_pg[0]["content"]
        
        # 4. Test Qdrant Search
        # Note: Qdrant strategy might need wait/retry in a real integration scenario
        res_qdrant = await replica.search("high-tech watch", engine="qdrant")
        assert len(res_qdrant) > 0
        assert product_name in res_qdrant[0]["content"]
        
        # Verify result consistency
        assert res_pg[0]["id"] == res_qdrant[0]["id"]

@pytest.mark.asyncio
async def test_search_alias_promotion():
    """
    Verify that Qdrant Aliases are updated when a version is promoted in Postgres.
    """
    import os
    
    # Use environment variables injected by Docker Compose
    source_url = os.environ.get("SOURCE_URL", "postgresql://postgres:password@source:5432/production_db")
    source_url_internal = os.environ.get("SUBSCRIPTION_SOURCE_URL", "postgresql://postgres:password@source:5432/production_db")
    sink_url = os.environ.get("SINK_URL", "local")

    qdrant = QdrantClient("http://localhost:6333")
    
    # 1. Start with Version 1
    async with connect(
        source_url=source_url,
        sink_url=sink_url,
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
        # Seed data to ensure outbox/mirrors have something to do
        async with await connect_db(source_url) as conn:
            await conn.execute("INSERT INTO products (name, description) VALUES ('V1 Product', 'Description')")
            await conn.commit()

        # Force migration
        async with await get_sink_conn() as conn:
            await conn.execute("ALTER TABLE _sink_mirror_registry ADD COLUMN IF NOT EXISTS promoted_version_id TEXT")
            await conn.commit()

        # Wait for promotion (Postgres)
        logger.info("Waiting for Postgres view products_search...")
        for i in range(30):
            try:
                async with await get_sink_conn() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT count(*) FROM products_search")
                        logger.info(f"Postgres view is ready (attempt {i})")
                        break
            except Exception: pass
            await asyncio.sleep(1)
        else:
            pytest.fail("Timed out waiting for Postgres view")
        
        # Verify Qdrant Alias points to V1
        logger.info("Waiting for Qdrant Alias alias_test_products_production...")
        v1_collection = None
        for i in range(30):
            try:
                aliases = qdrant.get_aliases().aliases
                found = next((a for a in aliases if a.alias_name == "alias_test_products_production"), None)
                if found:
                    v1_collection = found.collection_name
                    logger.info(f"Qdrant Alias discovered: {v1_collection}")
                    break
            except Exception: pass
            await asyncio.sleep(1)
        else:
            # Diagnostics: list all collections
            collections = [c.name for c in qdrant.get_collections().collections]
            pytest.fail(f"Timed out waiting for V1 Qdrant Alias. Available collections: {collections}")
            
        assert "alias_test_products_" in v1_collection

    # 2. Upgrade to Version 2 (Change formatting)
    async with connect(
        source_url=source_url,
        sink_url=sink_url,
        pipelines={
            "products": {
                "ingest": {
                    "table": "products",
                    "columns": ["id", "name", "description"],
                    "p_key": "id"
                },
                "pipeline": {
                    "template": "V2: $name $chunk",
                    "content_column": "description", # Must be explicit
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
        # Wait for promotion to V2 (Postgres)
        logger.info("Waiting for Postgres view promotion to V2...")
        for _ in range(30):
            try:
                async with await get_sink_conn() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT view_definition FROM information_schema.views WHERE table_name = 'products_search'")
                        row = await cur.fetchone()
                        if row and v1_collection not in row[0]: # View definition changed to new target
                            logger.info("Postgres view promoted to V2")
                            break
            except Exception: pass
            await asyncio.sleep(1)
        else:
             pytest.fail("Timed out waiting for Postgres V2 promotion")
        
        # Verify Qdrant Alias now points to V2
        logger.info("Waiting for Qdrant Alias to follow V1 -> V2...")
        for _ in range(30):
            aliases = qdrant.get_aliases().aliases
            found = next((a for a in aliases if a.alias_name == "alias_test_products_production"), None)
            if found and found.collection_name != v1_collection:
                logger.info(f"Qdrant Alias promoted to {found.collection_name}")
                break
            await asyncio.sleep(1)
            
        aliases = qdrant.get_aliases().aliases
        found = next((a for a in aliases if a.alias_name == "alias_test_products_production"), None)
        assert found.collection_name != v1_collection
        assert "alias_test_products_" in found.collection_name
        
        # Final check: search via alias (with retry to handle eventual searchability)
        logger.info("Verifying search via Qdrant Alias...")
        for _ in range(10):
            res = await replica.search("watch", engine="qdrant")
            if len(res) > 0:
                logger.info(f"Search success: found {len(res)} results")
                break
            await asyncio.sleep(1)
        else:
            pytest.fail("Timed out waiting for data to be searchable via Qdrant Alias")
