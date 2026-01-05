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
    source_url = "postgresql://postgres:password@127.0.0.1:5433/production_db"
    source_url_internal = "postgresql://postgres:password@source:5432/production_db"
    sink_url = "postgresql://postgres@127.0.0.1:5434/postgres"
    
    import os
    os.environ["SOURCE_URL"] = source_url
    os.environ["SINK_URL"] = sink_url
    os.environ["SUBSCRIPTION_SOURCE_URL"] = source_url_internal
    
    # Configure replica with a mirror
    async with connect(
        source_url=source_url,
        sink_url=sink_url,
        tables={
            "products": {
                "source_table": "products",
                "publication_columns": ["id", "name", "description"],
                "active": True,
                "mirrors": [
                    {
                        "id": "qdrant_meta",
                        "type": "qdrant",
                        "url": "http://localhost:6333",
                        "prefix": "strat_test_"
                    }
                ]
            }
        },
        sync=True
    ) as replica:
        # 0. Aggressive Cleanup (Inside pool)
        qdrant = QdrantClient("http://localhost:6333")
        try:
            collections = qdrant.get_collections().collections
            for col in collections:
                if col.name.startswith("strat_test_") or col.name.startswith("alias_test_"):
                    qdrant.delete_collection(col.name)
        except Exception: pass

        async with await get_sink_conn() as conn:
            # Force migration just in case
            await conn.execute("ALTER TABLE _sink_mirror_registry ADD COLUMN IF NOT EXISTS promoted_version_id TEXT")
            await conn.execute("TRUNCATE _sink_outbox CASCADE")
            await conn.execute("TRUNCATE _sink_mirror_registry CASCADE")
            await conn.commit()
            
        # 1. Seed data
        unique_id = str(uuid.uuid4())[:8]
        product_name = f"Strategy Watch {unique_id}"
        async with await connect_db(source_url) as conn:
            await conn.execute("DELETE FROM products")
            await conn.execute(
                "INSERT INTO products (name, description) VALUES (%s, %s)",
                (product_name, "A high-tech watch for strategic planning.")
            )
            await conn.commit()

        # 2. Wait for sync...
        product_name = f"Strategy Watch {unique_id}"
        for _ in range(30):
            status = await replica.get_status()
            pending = sum(v.get("pending_items", 0) for v in status.get("vectorizers", []))
            
            found_in_qdrant = False
            try:
                collections = qdrant.get_collections().collections
                for col in collections:
                    if col.name.startswith("strat_test_products_"):
                        points = qdrant.scroll(collection_name=col.name, limit=10)[0]
                        if any(product_name in (p.payload.get("content") or "") for p in points):
                            found_in_qdrant = True
                            break
            except Exception: pass
            
            # Check view as well
            view_exists = False
            try:
                async with await get_sink_conn() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT count(*) FROM products_search")
                        row = await cur.fetchone()
                        if row and row[0] > 0:
                            # Verify view content is fresh
                            await cur.execute("SELECT * FROM products_search LIMIT 1")
                            res = await cur.fetchone()
                            if res and product_name in str(res):
                                view_exists = True
            except Exception: pass
            
            logger.info(f"Wait status: pending={pending}, qdrant={found_in_qdrant}, view={view_exists}")
            
            if pending == 0 and found_in_qdrant and view_exists:
                break
            await asyncio.sleep(2)
        else:
            pytest.fail(f"Timed out waiting for sync. State: pending={pending}, qdrant={found_in_qdrant}, view={view_exists}")

        # 3. Test Postgres Search
        res_pg = await replica.search("high-tech watch")
        assert len(res_pg) > 0
        assert product_name in res_pg[0]["content"]
        
        # 4. Test Qdrant Search
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
    source_url = "postgresql://postgres:password@127.0.0.1:5433/production_db"
    source_url_internal = "postgresql://postgres:password@source:5432/production_db"
    sink_url = "postgresql://postgres@127.0.0.1:5434/postgres"
    
    import os
    os.environ["SOURCE_URL"] = source_url
    os.environ["SINK_URL"] = sink_url
    os.environ["SUBSCRIPTION_SOURCE_URL"] = source_url_internal

    qdrant = QdrantClient("http://localhost:6333")
    
    # 1. Start with Version 1
    async with connect(
        source_url=source_url,
        sink_url=sink_url,
        tables={
            "products": {
                "source_table": "products",
                "publication_columns": ["id", "name", "description"],
                "formatting_template": "V1: $name $chunk",
                "active": True,
                "mirrors": [{"id": "m1", "type": "qdrant", "url": "http://localhost:6333", "prefix": "alias_test_"}]
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
        tables={
            "products": {
                "source_table": "products",
                "publication_columns": ["id", "name", "description"],
                "formatting_template": "V2: $name $chunk",
                "active": True,
                "mirrors": [{"id": "m1", "type": "qdrant", "url": "http://localhost:6333", "prefix": "alias_test_"}]
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
        
        # Final check: search via alias
        res = await replica.search("watch", engine="qdrant")
        assert len(res) > 0
