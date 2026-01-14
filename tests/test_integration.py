import pytest
import asyncio
import logging
from pgvector.psycopg import register_vector_async as register_vector  # type: ignore
from pg_replica import PGSearchReplica, settings as global_settings
from pg_replica.database import dict_row

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_full_replication_flow(clean_db, robust_slot_cleanup, internal_source_url, source_conn, sink_conn, wait_for_pgai_sync):
    """Integration test for basic multi-table logic (one table)."""
    from unittest.mock import patch
    
    custom_settings = {
        "pipelines": {
            "products": {
                "ingest": {"table": "full_products", "columns": ["name", "description"]},
                "pipeline": {"template": "$chunk $name", "content_column": "description", "chunking": {"strategy": "recursive_character"}, "embedding": {"provider": "ollama", "model": "nomic-embed-text", "dimension": 768}},
                "storage": {"postgres": {"profile": "vector"}},
                "active": True
            }
        }
    }
    
    # Clean specific slot for this test
    await robust_slot_cleanup("sub_products")

    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": internal_source_url}):
        # 1. Setup Source
        await source_conn.execute("DROP TABLE IF EXISTS full_products CASCADE")
        await source_conn.execute("CREATE TABLE full_products (id SERIAL PRIMARY KEY, name TEXT, description TEXT)")
        await source_conn.execute("INSERT INTO full_products (name, description) VALUES ('SuperGadget', 'A really useful tool for testing')")

        # 2. Cleanup Sink Specifics
        await sink_conn.execute("DROP TABLE IF EXISTS full_products CASCADE")
        await sink_conn.execute("DELETE FROM _replica_state WHERE key = 'sub_products'")

        # 3. Run Replicator
        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            settings = replica.settings
            config = settings.pipelines["products"]

            # 4. Verify Raw Replication
            found = False
            for _ in range(10):
                import psycopg
                async with sink_conn.cursor() as cur:
                    # Robust wait for table creation
                    for _ in range(30):
                        try:
                            await cur.execute(f"SELECT count(*) FROM {config.ingest.table} WHERE name = 'SuperGadget'")
                            if (await cur.fetchone())[0] > 0:
                                found = True
                                break
                        except psycopg.errors.UndefinedTable:
                            pass
                        await asyncio.sleep(1)
            assert found, "Native replication failed"

            # 5. Verify pgai Vectorization
            assert await wait_for_pgai_sync(settings, "products")
            
            # 6. Verify Search
            results = await replica.search("SuperGadget", table="products")
            assert len(results) > 0
            assert "SuperGadget" in results[0]["content"]


@pytest.mark.asyncio
async def test_multi_table_search(clean_db, robust_slot_cleanup, internal_source_url, source_conn, sink_conn, wait_for_pgai_sync):
    """Verify that multiple tables can be searched independently."""
    from unittest.mock import patch
    custom_settings = {
        "pipelines": {
            "t1": {
                "ingest": {"table": "table1", "columns": ["content"], "p_key": "id"},
                "pipeline": {"template": "$chunk $content", "content_column": "content", "chunking": {"strategy": "recursive_character"}, "embedding": {"provider": "ollama", "model": "nomic-embed-text", "dimension": 768}},
                "storage": {"postgres": {"profile": "vector"}}
            },
            "t2": {
                "ingest": {"table": "table2", "columns": ["content"], "p_key": "id"},
                "pipeline": {"template": "$chunk $content", "content_column": "content", "chunking": {"strategy": "recursive_character"}, "embedding": {"provider": "ollama", "model": "nomic-embed-text", "dimension": 768}},
                "storage": {"postgres": {"profile": "vector"}}
            }
        }
    }
    
    for t in ["sub_t1", "sub_t2"]: await robust_slot_cleanup(t)

    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": internal_source_url}):
        # Setup Source
        for t in ["table1", "table2"]:
            await source_conn.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
            await source_conn.execute(f"CREATE TABLE {t} (id SERIAL PRIMARY KEY, name TEXT, description TEXT, content TEXT)")
        await source_conn.execute("INSERT INTO table1 (name, description, content) VALUES ('N1', 'D1', 'alpha')")
        await source_conn.execute("INSERT INTO table2 (name, description, content) VALUES ('N2', 'D2', 'beta')")

        # Cleanup Sink
        await sink_conn.execute("DROP TABLE IF EXISTS table1 CASCADE")
        await sink_conn.execute("DROP TABLE IF EXISTS table2 CASCADE")
        await sink_conn.execute("DELETE FROM _replica_state WHERE key IN ('sub_t1', 'sub_t2')")

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            assert await wait_for_pgai_sync(replica.settings, "t1")
            assert await wait_for_pgai_sync(replica.settings, "t2")

            r1 = await replica.search("alpha", table="t1")
            assert len(r1) >= 1
            assert "alpha" in r1[0]["content"]

            r2 = await replica.search("beta", table="t2")
            assert len(r2) >= 1
            assert "beta" in r2[0]["content"]


@pytest.mark.asyncio
async def test_hybrid_search_rrf(clean_db, robust_slot_cleanup, internal_source_url, source_conn, sink_conn, wait_for_pgai_sync):
    """Verify that hybrid search (RRF) view is created and contains ts_col."""
    from unittest.mock import patch
    custom_settings = {
        "pipelines": {
            "hybrid": {
                "ingest": {"table": "hybrid_products", "columns": ["name", "description", "content"], "p_key": "id"},
                "pipeline": {"template": "$chunk $content", "content_column": "content", "chunking": {"strategy": "recursive_character"}, "embedding": {"provider": "ollama", "model": "nomic-embed-text", "dimension": 768}},
                "storage": {"postgres": {"profile": "hybrid"}}
            }
        }
    }
    
    await robust_slot_cleanup("sub_hybrid")
    
    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": internal_source_url}):
        await source_conn.execute("DROP TABLE IF EXISTS hybrid_products CASCADE")
        await source_conn.execute("CREATE TABLE hybrid_products (id SERIAL PRIMARY KEY, name TEXT, description TEXT, content TEXT)")
        await source_conn.execute("INSERT INTO hybrid_products (name, description, content) VALUES ('H1', 'D1', 'hybrid search test')")

        await sink_conn.execute("DROP TABLE IF EXISTS hybrid_products CASCADE")
        await sink_conn.execute("DELETE FROM _replica_state WHERE key = 'sub_hybrid'")

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            assert await wait_for_pgai_sync(replica.settings, "hybrid")
            
            async with sink_conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT * FROM hybrid_search LIMIT 1")
                row = await cur.fetchone()
                assert "ts_col" in row, "ts_col missing from hybrid view"
                assert row["ts_col"] is not None


@pytest.mark.asyncio
async def test_blue_green_swap(clean_db, robust_slot_cleanup, internal_source_url, source_conn, sink_conn, wait_for_pgai_sync):
    """Verify atomic swap with multi-table config."""
    from unittest.mock import patch
    base_config = {
        "pipelines": {
            "swap": {
                "ingest": {"table": "swap_products", "columns": ["name", "description", "content"], "p_key": "id"},
                "pipeline": {"template": "$chunk $content", "content_column": "content", "chunking": {"strategy": "recursive_character"}, "embedding": {"provider": "ollama", "model": "nomic-embed-text", "dimension": 768}},
                "storage": {"postgres": {"profile": "vector"}}
            }
        }
    }
    
    await robust_slot_cleanup("sub_swap")
    
    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": internal_source_url}):
        await source_conn.execute("DROP TABLE IF EXISTS swap_products CASCADE")
        await source_conn.execute("CREATE TABLE swap_products (id SERIAL PRIMARY KEY, name TEXT, description TEXT, content TEXT)")
        await source_conn.execute("INSERT INTO swap_products (name, description, content) VALUES ('N', 'D', 'initial content')")

        await sink_conn.execute("DROP TABLE IF EXISTS swap_products CASCADE")
        await sink_conn.execute("DELETE FROM _replica_state WHERE key = 'sub_swap'")
        await sink_conn.execute("DROP VIEW IF EXISTS swap_search CASCADE")

        # Phase 1: Deploy V1
        async with PGSearchReplica(sync=True, **base_config) as replica:
            assert await wait_for_pgai_sync(replica.settings, "swap")
            
            async with sink_conn.cursor() as cur:
                await cur.execute("SELECT table_name FROM information_schema.view_table_usage WHERE view_name = 'swap_search' AND table_name LIKE '%%_embedding_v%%'")
                row = await cur.fetchone()
                v1 = row[0] if row else None
            
        # Phase 2: Deploy V2 (Trigger Swap)
        new_config = {
            "pipelines": {
                "swap": {
                    "ingest": {"table": "swap_products", "columns": ["name", "description", "content"], "p_key": "id"},
                    "pipeline": {"template": "$chunk NEW $content", "content_column": "content", "chunking": {"strategy": "recursive_character"}, "embedding": {"provider": "ollama", "model": "nomic-embed-text", "dimension": 768}},
                    "storage": {"postgres": {"profile": "vector"}}
                }
            }
        }
        async with PGSearchReplica(sync=True, **new_config) as replica:
            # Reconciler runs on startup
            # Give the orchestrator time to pick up the change and reconcile
            v2 = None
            for _ in range(30):
                async with sink_conn.cursor() as cur:
                    await cur.execute("SELECT table_name FROM information_schema.view_table_usage WHERE view_name = 'swap_search' AND table_name LIKE '%%_embedding_v%%'")
                    row = await cur.fetchone()
                    current_v = row[0] if row else None
                    if current_v and current_v != v1:
                        v2 = current_v
                        break
                await asyncio.sleep(1)
                
            assert v1 is not None and v2 is not None, f"Metadata views not created (v1={v1}, v2={v2})"
            assert v1 != v2, "Blue-Green swap failed"
