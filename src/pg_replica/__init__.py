from .client import PGSearchReplica
from .config import settings

__all__ = ["PGSearchReplica", "settings", "connect"]

def connect(sink_url: str = "local", sync: bool = False, **kwargs) -> PGSearchReplica:
    """
    Convenience helper to create a PGSearchReplica instance.
    Safe entry point. Defaulting sync=False prevents accidental 
    infrastructure orchestration.
    """
    kwargs["sink_url"] = sink_url
    kwargs["sync"] = sync
    return PGSearchReplica(**kwargs)
