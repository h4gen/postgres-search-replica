import logging
import asyncio
from typing import AsyncGenerator
from contextlib import asynccontextmanager
from psycopg_pool import AsyncConnectionPool
from ..config import SourceConfig
from .base import BaseSourceAdapter

logger = logging.getLogger(__name__)

class PostgresSourceAdapter(BaseSourceAdapter):
    def __init__(self, name: str, config: SourceConfig):
        self.name = name
        self.config = config
        self.pool: AsyncConnectionPool | None = None

    async def connect(self) -> None:
        if self.pool:
            return
            
        logger.info(f"Initializing source connection pool '{self.name}' with {self.config.connection_url}...")
        self.pool = AsyncConnectionPool(
            conninfo=self.config.connection_url,
            min_size=1,
            max_size=5,
            open=False,
        )
        await self.pool.open()

    async def close(self) -> None:
        if self.pool:
            logger.info(f"Closing source connection pool '{self.name}'...")
            try:
                await self.pool.close()
            except (asyncio.CancelledError, Exception) as e:
                logger.debug(f"Error closing pool '{self.name}': {e}")
            self.pool = None

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[any, None]: # type: ignore
        if not self.pool:
            raise RuntimeError(f"Source pool '{self.name}' not initialized")
        async with self.pool.connection() as conn:
            yield conn
