import asyncio
import logging
from typing import Any, Optional

from .config import settings
from .database import connect_db
from .orchestrator import Orchestrator

logger = logging.getLogger(__name__)


class PGSearchReplica:
    """
    The unified entry point for Postgres Search Replica.
    Handles both infrastructure (replication, workers) and querying.
    """

    def __init__(self, **kwargs):
        """
        Initialize with optional configuration overrides.
        Example: PGSearchReplica(sink_url="local", source_url="...")
        """
        for key, value in kwargs.items():
            if hasattr(settings, key):
                setattr(settings, key, value)

        self._orchestrator: Optional[Orchestrator] = None
        self._conn = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def start(self, sync: bool = True):
        """
        Start the replica.
        If sync=True, starts the background replication and workers.
        """
        if sync:
            logger.info("Starting PGSearchReplica in Sync Mode...")
            self._orchestrator = Orchestrator()
            await self._orchestrator.start()
        else:
            logger.info(
                "Starting PGSearchReplica in Client Mode (Query only)..."
            )

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
            self._conn = await connect_db(settings.resolved_sink_url)
            # Register vector types
            from pgvector.psycopg import register_vector_async

            await register_vector_async(self._conn)
        return self._conn

    async def search(
        self, query: str, limit: int = 5, table: Optional[str] = None
    ) -> list[dict[str, Any]]:
        """
        Perform a semantic search.

        Args:
            query: The text to search for.
            limit: Number of results to return.
            table: Optional override for the replica table name.
        """
        target_table = table or settings.sink_replica_table

        # 1. Get embedding in Python (Clean, fast, no Postgres hacks)
        import os
        from ollama import AsyncClient

        ollama_host = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
        client = AsyncClient(host=ollama_host)

        # Use the same model as configured for the vectorizer
        res = await client.embeddings(
            model=settings.embedding_model, prompt=query
        )
        embedding = res["embedding"]

        # 2. Simple vector search query
        conn = await self._get_conn()
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT 
                    {settings.id_column}, 
                    {settings.target_content_column},
                    {settings.embedding_column} <=> %s::vector as distance
                FROM {target_table}
                ORDER BY distance ASC
                LIMIT %s
            """,
                (embedding, limit),
            )

            rows = await cur.fetchall()
            results = []
            for row in rows:
                results.append(
                    {"id": row[0], "content": row[1], "distance": float(row[2])}
                )
            return results

    async def get_status(self) -> dict[str, Any]:
        """Get the current replication and embedding status."""
        conn = await self._get_conn()
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM ai.vectorizer_status")
            status_rows = await cur.fetchall()

            await cur.execute("SELECT * FROM ai.vectorizer_errors")
            error_rows = await cur.fetchall()

            return {"vectorizers": status_rows, "errors": error_rows}
