from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any

class BaseSourceAdapter(ABC):
    """
    Abstract Base Class for Source Adapters.
    Defines the interface for connecting to and interacting with a data source.
    """
    
    @abstractmethod
    async def connect(self) -> None:
        """Initialize the connection or connection pool."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close the connection or connection pool."""
        pass

    @abstractmethod
    async def get_connection(self) -> AsyncGenerator[Any, None]:
        """
        Yields a usable connection object (e.g. psycopg.AsyncConnection).
        Must be used as an async context manager.
        """
        yield None

    @abstractmethod
    async def discovery_state(self) -> dict:
        """Discover the current state of the source (e.g., LSN, high-watermark, publications)."""
        pass

    @abstractmethod
    async def prepare_sync(self, config_name: str, **kwargs) -> None:
        """Prepare the source for synchronization (e.g., create slots/publications or verify indices)."""
        pass

    async def monitor_lag(self, target_name: str, max_size_mb: float) -> float:
        """Monitor replication lag and take action if it exceeds thresholds. Returns lag in MB."""
        return 0.0
