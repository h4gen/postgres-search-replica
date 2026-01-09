from typing import Dict, Optional, Type
import logging
from ..config import SourceConfig, Settings
from .base import BaseSourceAdapter
from .postgres import PostgresCDCAdapter, PostgresPollingAdapter

logger = logging.getLogger(__name__)

# Registry of initialized adapters
_adapters: Dict[str, BaseSourceAdapter] = {}

def get_adapter_class(type_name: str, strategy: Optional[str] = None) -> Type[BaseSourceAdapter]:
    """Factory mapping config type and strategy to Adapter Class."""
    if type_name == "postgres":
        if strategy == "cdc":
            return PostgresCDCAdapter
        elif strategy == "polling":
            return PostgresPollingAdapter
    elif type_name == "local":
        from .local import LocalFileAdapter
        return LocalFileAdapter
    elif type_name == "s3":
        from .s3 import S3SourceAdapter
        return S3SourceAdapter
    raise ValueError(f"Unknown source type/strategy: {type_name}/{strategy}")

async def init_source_adapters(settings: Settings) -> None:
    """Initialize all source adapters defined in settings."""
    global _adapters
    
    for name, config in settings.sources.items():
        if name not in _adapters:
            strategy = getattr(config, "strategy", None)
            adapter_cls = get_adapter_class(config.type, strategy)
            adapter = adapter_cls(name, config) # type: ignore
            await adapter.connect()
            _adapters[name] = adapter

async def close_source_adapters() -> None:
    """Close all source adapters."""
    global _adapters
    for name, adapter in _adapters.items():
        await adapter.close()
    _adapters = {}

def get_source_adapter(name: str) -> BaseSourceAdapter:
    """Get an initialized adapter by name."""
    if name not in _adapters:
         # Fallback for legacy/test scenarios usage of 'default' implicit creation
         # This mirrors the logic previously in get_source_conn
        raise RuntimeError(f"Source adapter '{name}' not initialized. Available: {list(_adapters.keys())}")
    return _adapters[name]

# Helper for direct connection access (bridge for existing code)
async def get_source_connection(name: str):
    """
    Bridge helper to get a raw connection context manager.
    Usage: async with get_source_connection("default") as conn: ...
    """
    adapter = get_source_adapter(name)
    return adapter.get_connection()
