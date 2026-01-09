import pytest
import asyncio
from pathlib import Path
from pg_replica.config import SearchPipeline, IngestConfig, LocalSourceConfig, PipelineConfig, EmbeddingConfig, ParsingConfig
from pg_replica import settings as global_settings
from pg_replica.source import init_source_adapters, close_source_adapters
from pg_replica.reconciler import Reconciler
from pg_replica.database import get_sink_conn, init_pools, close_pools

@pytest.fixture
def settings():
    return global_settings.model_copy(deep=True)

@pytest.fixture
def settings_with_local(settings):
    data_dir = Path(__file__).parent / "fixtures" / "local_source"
    # uri_prefix allows the container (sink) to find files mounted at /app
    # while the host (test runner) discovers them via host paths.
    settings.sources["local_src"] = LocalSourceConfig(
        path=str(data_dir.absolute()),
        uri_prefix="file:///app/tests/fixtures/local_source"
    )
    return settings

@pytest.mark.asyncio
async def test_local_file_sync_e2e(settings_with_local, sink_conn, clean_db, wait_for_pgai_sync):
    """
    Test end-to-end sync from a local directory to a Postgres sink table.
    """
    settings = settings_with_local
    data_dir = Path(settings.sources["local_src"].path)
    
    # 0. Clean up any stale data from previous failed starts
    async with sink_conn.cursor() as cur:
        await cur.execute("DROP TABLE IF EXISTS file_sync_target CASCADE")

    pipeline = SearchPipeline(
        ingest=IngestConfig(
            source="local_src",
            table="file_sync_target",
            columns=["uri", "size", "last_modified", "etag"],
            p_key="uri"
        ),
        pipeline=PipelineConfig(
            template="$chunk",
            content_column="uri",
            parsing=ParsingConfig(strategy="auto"),
            embedding=EmbeddingConfig(provider="ollama", model="nomic-embed-text", dimension=768)
        ),
        active=True
    )
    settings.pipelines["p_files"] = pipeline
    
    # Initialize pools (this includes source adapters)
    await init_pools(settings)
    
    try:
        reconciler = Reconciler(settings)
        # Reconcile will trigger ACTION_TYPE.SINK_RECOVERY for the new pipeline
        await reconciler.reconcile()
        
        # Verify sink table metadata
        async with await get_sink_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT count(*) FROM file_sync_target")
                res = await cur.fetchone()
                assert res[0] == 2
                
                await cur.execute("SELECT uri FROM file_sync_target ORDER BY uri")
                rows = await cur.fetchall()
                uris = [row[0] for row in rows]
                assert any("sample.md" in u for u in uris)
                assert any("sample.txt" in u for u in uris)

                # Verify vectorizer creation (check if ai.vectorizer exists and has our vectorizer)
                version_id = pipeline.get_version_id()
                expected_vectorizer = f"file_sync_target_store_v{version_id}"
                await cur.execute("SELECT count(*) FROM ai.vectorizer WHERE name = %s", (expected_vectorizer,))
                res = await cur.fetchone()
                assert res[0] == 1

                # 4. VERIFY CONTENT PARSING AND SEARCH
                # Wait for embeddings to be generated
                # expected items: sample.md (1-2 chunks) + sample.txt (1 chunk)
                assert await wait_for_pgai_sync(settings, "p_files", expected_count=2, timeout=60)

                # Verify actual content in chunks
                search_view = "p_files_search"
                await cur.execute(f"SELECT chunk FROM {search_view} WHERE chunk LIKE '%markdown%'")
                row = await cur.fetchone()
                assert row is not None, "Markdown content not found in chunks!"

                await cur.execute(f"SELECT chunk FROM {search_view} WHERE chunk LIKE '%plain text%'")
                row = await cur.fetchone()
                assert row is not None, "Text content not found in chunks!"

                # Perform a basic vector search (keyword match via view is enough to prove it's there, 
                # but we can also check the embedding column)
                await cur.execute(f"SELECT uri, chunk FROM {search_view} ORDER BY embedding <-> (SELECT embedding FROM {expected_vectorizer} LIMIT 1) LIMIT 1")
                row = await cur.fetchone()
                assert row is not None
                assert len(row[1]) > 10 # Meaningful chunk text
                
    finally:
        await close_pools()
