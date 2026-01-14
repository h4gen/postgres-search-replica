import asyncio
import logging
import pytest
from qdrant_client import QdrantClient
from pg_replica import connect
from pg_replica.database import get_sink_conn, dict_row, init_pools, close_pools

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_universal_outbox_capture_and_sync():
    """
    End-to-end test for Universal Outbox:
    WAL -> _raw -> pgai -> _sink_outbox -> Qdrant
    """
    # Host perspective: source is at 5433, sink is at 5434
    source_url_host = "postgresql://postgres:password@127.0.0.1:5433/production_db"
    # Sink perspective (inside docker): source is at 'source:5432'
    source_url_internal = "postgresql://postgres:password@source:5432/production_db"
    
    # Sink URL as seen from host
    sink_url_host = "postgresql://postgres@127.0.0.1:5434/postgres"
    
    import os
    os.environ["SOURCE_URL"] = source_url_host
    os.environ["SINK_URL"] = sink_url_host
    os.environ["SUBSCRIPTION_SOURCE_URL"] = source_url_internal
    
    # Configure replica
    async with connect(
        source_url=source_url_host,
        sink_url=sink_url_host,
        pipelines={
            "products": {
                "ingest": {
                    "table": "products",
                    "columns": ["id", "name", "description"],
                    "p_key": "id"
                },
                "pipeline": {
                    "template": "Title: $name\nContent: $chunk",
                    "content_column": "description",
                    "embedding": {
                         "provider": "ollama",
                         "model": "nomic-embed-text",
                         "dimension": 768
                    }
                },
                "storage": {
                    "postgres": { "profile": "hybrid" },
                    "mirrors": [
                        {
                            "id": "qdrant_test",
                            "type": "qdrant",
                            "config": {
                                "url": "http://localhost:6333",
                                "prefix": "test_"
                            }
                        }
                    ]
                },
                "active": True
            }
        },
        sync=True
    ) as replica:
        # Initialize pools for backend function usage in test process (get_sink_conn)
        await init_pools(replica.settings)
        try:
            # 1. Wait for infrastructure to be ready
            logger.info("Waiting for replica to be ready...")
            await asyncio.sleep(5) 
        
            # 2. Insert data into Source
            async with await connect_db(source_url_host) as conn:
                await conn.execute(
                    "INSERT INTO products (name, description) VALUES (%s, %s)",
                    ("Outbox Test Product", "This product should end up in the outbox and then in Qdrant.")
                )
                await conn.commit()

            # 3. Verify transactional capture into _sink_outbox
            # This will fail initially because _sink_outbox and triggers don't exist
            logger.info("Checking _sink_outbox for captured changes...")
            found_in_outbox = False
            for _ in range(30):
                try:
                    async with await get_sink_conn() as conn:
                        async with conn.cursor(row_factory=dict_row) as cur:
                            await cur.execute("SELECT count(*) FROM _sink_outbox")
                            count = (await cur.fetchone())["count"]
                            if count > 0:
                                found_in_outbox = True
                                break
                except Exception as e:
                    logger.debug(f"Outbox not ready yet: {e}")
                await asyncio.sleep(2)
            
            assert found_in_outbox, "Data was not captured in _sink_outbox"

            # 4. Verify downstream delivery to Qdrant
            logger.info("Checking Qdrant for synced vectors...")
            qdrant = QdrantClient("http://localhost:6333")
            
            found_in_qdrant = False
            for _ in range(30):
                try:
                    # We expect a collection named 'products_v...'
                    collections = qdrant.get_collections().collections
                    for col in collections:
                        if col.name.startswith("test_products_"):
                            points = qdrant.scroll(collection_name=col.name, limit=1)[0]
                            if points:
                                found_in_qdrant = True
                                break
                    if found_in_qdrant: break
                except Exception: pass
                await asyncio.sleep(2)

            assert found_in_qdrant, "Data was not synced to Qdrant"
            
        finally:
            await close_pools()

async def connect_db(url):
    import psycopg
    return await psycopg.AsyncConnection.connect(url)
