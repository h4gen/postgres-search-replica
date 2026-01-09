import logging
import os
from typing import Any, Optional, List, Dict
from ollama import AsyncClient

from .config import settings as global_settings
from .database import connect_db, dict_row
from .orchestrator import Orchestrator

logger = logging.getLogger(__name__)


class PGSearchReplica:
    """
    The unified entry point for Postgres Search Replica.
    Handles both infrastructure (replication, workers) and querying.
    """

    def __init__(self, sync: bool = False, **kwargs):
        """
        Initialize with optional configuration overrides.
        """
        import copy
        # Isolate settings per instance and ensure validation runs
        self.settings = global_settings.__class__.model_validate(
            {**copy.deepcopy(global_settings.model_dump()), **kwargs}
        )
        self._sync_mode = sync
        self._orchestrator: Optional[Orchestrator] = None
        self._conn = None

    async def __aenter__(self):
        await self.start(sync=self._sync_mode)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def start(self, sync: Optional[bool] = None):
        """
        Start the replica.
        If sync=True, starts the background replication and workers.
        """
        use_sync = sync if sync is not None else self._sync_mode
        if use_sync:
            logger.info("Starting PGSearchReplica in Sync Mode...")
            self._orchestrator = Orchestrator(self.settings)
            await self._orchestrator.start()
        else:
            logger.info("Starting PGSearchReplica in Client Mode (Query only)...")

    async def stop(self):
        """Stop the replica and all background services."""
        if self._orchestrator:
            await self._orchestrator.stop()
            self._orchestrator = None

        if self._conn:
            await self._conn.close()
            self._conn = None

    async def _get_conn(self):
        if not self._conn or self._conn.closed:
            self._conn = await connect_db(self.settings.resolved_sink_url)
            from pgvector.psycopg import register_vector_async
            await register_vector_async(self._conn)
        return self._conn

    async def search(
        self, query: str, limit: int = 5, table: Optional[str] = None, engine: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Perform a semantic or hybrid search.

        Args:
            query: The text to search for.
            limit: Number of results to return.
            table: The name of the table configuration to use.
            engine: The search engine to use (postgres, qdrant, pinecone). 
                    Defaults to the one in TableConfig.
        """
        target_name = table or next(iter(self.settings.pipelines))
        if target_name not in self.settings.pipelines:
            raise ValueError(f"Table configuration '{target_name}' not found.")
        
        # In the new schema, we default to postgres unless mirrors are the primary target
        # For this shim, we'll keep the engine choice logic
        config = self.settings.pipelines[target_name]
        # In v7, engine is determined by usage, but let's assume postgres for now if not explicit
        search_engine = engine or "postgres"

        # 1. Get embedding in Python
        ollama_host = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
        client = AsyncClient(host=ollama_host)
        res = await client.embeddings(model=config.pipeline.embedding.model, prompt=query)
        embedding = res["embedding"]

        # 2. Execute via strategy
        from .strategies import PostgresSearchStrategy, QdrantSearchStrategy
        
        strategies = {
            "postgres": PostgresSearchStrategy(),
            "qdrant": QdrantSearchStrategy(),
        }
        
        strategy = strategies.get(search_engine)
        if not strategy:
            raise ValueError(f"Unsupported search engine: {search_engine}")
            
        return await strategy.search(
            query=query,
            embedding=embedding,
            limit=limit,
            config=config,
            conn_provider=self._get_conn,
            target_name=target_name,
            settings=self.settings
        )

    async def get_status(self) -> dict[str, Any]:
        """Get current status for all configured tables."""
        conn = await self._get_conn()
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT * FROM ai.vectorizer_status")
            status = await cur.fetchall()
            await cur.execute("SELECT * FROM ai.vectorizer_errors")
            errors = await cur.fetchall()
            return {"vectorizers": status, "errors": errors}
