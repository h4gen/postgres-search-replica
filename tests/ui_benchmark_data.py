import asyncio
import uuid
import logging
from pg_replica.config import Settings, TableConfig
from pg_replica.database import (
    init_pools, 
    get_source_conn, 
    close_pools,
    ensure_config_history_table,
    save_table_config
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MOCK_DOCS = [
    ("Introduction to Distributed AI", "Distributed AI focuses on large-scale systems where multiple agents cooperate..."),
    ("Scalable Machine Learning Systems", "Building scalable ML systems requires efficient data sharding and parallelized training..."),
    ("Federated Learning Overview", "Federated learning allows training on decentralized data while maintaining privacy..."),
    ("Vector Databases for LLMs", "Vector databases like Qdrant and pgvector are essential for high-speed retrieval..."),
    ("The Evolution of SQL in Search", "Modern SQL databases are increasingly integrating semantic search capabilities..."),
    ("Deep Dive into RAG Architectures", "Retrieval Augmented Generation (RAG) combines search with LLMs for factual accuracy..."),
    ("Optimizing Postgres for Search", "Index performance in Postgres can be improved using GIN, GiST and now HNSW..."),
    ("AI-Driven Automation in Finance", "Financial institutions are leveraging AI for fraud detection and risk assessment..."),
]

async def seed_source_db(settings: Settings):
    """Seed the production database with mock data."""
    logger.info("Seeding source database with mock documents...")
    async with await get_source_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute("CREATE TABLE IF NOT EXISTS production_docs (id UUID PRIMARY KEY, title TEXT, content TEXT)")
            await cur.execute("TRUNCATE production_docs")
            for title, content in MOCK_DOCS:
                await cur.execute(
                    "INSERT INTO production_docs (id, title, content) VALUES (%s, %s, %s)",
                    (str(uuid.uuid4()), title, content)
                )

async def setup_benchmarks():
    """Run a full setup to create 'Baseline' and 'Experimental' branches."""
    settings = Settings()
    await init_pools(settings)
    
    try:
        # 1. Seed Source
        await seed_source_db(settings)
        
        # 2. Setup a 'Live' Baseline (v1.2)
        base_config = TableConfig(
            source_table="production_docs",
            publication_columns=["id", "title", "content"],
            embedding_model="nomic-embed-text",
            search_profile="hybrid"
        )
        
        # PERSIST config so UI and Reconciler see it
        logger.info("Registering 'production_docs' config in Control Plane...")
        await ensure_config_history_table(settings)
        await save_table_config(settings, "production_docs", base_config)
        
        logger.info("UI Benchmark data seeded and registered successfully.")
    finally:
        await close_pools()

if __name__ == "__main__":
    asyncio.run(setup_benchmarks())
