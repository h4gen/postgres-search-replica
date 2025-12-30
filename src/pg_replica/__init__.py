from .client import PGSearchReplica
from .config import settings

__all__ = ["PGSearchReplica", "settings", "connect"]

def connect(sink_url: str = "local", **kwargs) -> PGSearchReplica:
    """
    Convenience helper to create a PGSearchReplica instance.
    """
    kwargs["sink_url"] = sink_url
    return PGSearchReplica(**kwargs)

