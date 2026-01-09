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
    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[Any, None]:
        """
        Yields a usable connection object (e.g. psycopg.AsyncConnection).
        Must be used as an async context manager.
        """
        yield None
