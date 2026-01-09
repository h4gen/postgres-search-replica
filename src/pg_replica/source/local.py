import os
import logging
from pathlib import Path
from datetime import datetime
from typing import AsyncGenerator, Dict, Any, List
from contextlib import asynccontextmanager
from .base import BaseSourceAdapter
from ..config import LocalSourceConfig

logger = logging.getLogger(__name__)

class LocalFileAdapter(BaseSourceAdapter):
    def __init__(self, name: str, config: LocalSourceConfig):
        self.name = name
        self.config = config
        self.root_path = Path(config.path)

    async def connect(self) -> None:
        if not self.root_path.exists():
             raise FileNotFoundError(f"Local source path does not exist: {self.root_path}")

    async def close(self) -> None:
        pass

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[Path, None]:
        yield self.root_path

    async def discovery_state(self) -> dict:
        try:
            files = list(self.root_path.glob("**/*"))
            return {
                "is_reachable": True,
                "path": str(self.root_path),
                "files_count": len([f for f in files if f.is_file()])
            }
        except Exception as e:
            return {"is_reachable": False, "error": str(e)}

    async def prepare_sync(self, config_name: str, **kwargs) -> None:
        pass

    async def get_table_columns(self, table_name: str) -> Dict[str, str]:
        return {
            "uri": "TEXT",
            "size": "BIGINT",
            "last_modified": "TIMESTAMP",
            "etag": "TEXT"
        }

    async def fetch_batch(self, table_name: str, columns: List[str], p_key: str, last_id: Any, batch_size: int, filter_str: str = None) -> List[Dict[str, Any]]:
        batch = []
        all_uris = []
        
        # We need a stable ordering. Building the full list might be slow for massive dirs, 
        # but for this scale it's fine.
        for root, _, files in os.walk(self.root_path):
            for file in files:
                file_path = Path(root) / file
                if self.config.uri_prefix:
                    rel_path = file_path.relative_to(self.root_path)
                    # Building the URI by appending the relative posix path to the prefix
                    uri = f"{self.config.uri_prefix.rstrip('/')}/{rel_path.as_posix()}"
                else:
                    uri = file_path.absolute().as_uri()
                
                if last_id and uri <= last_id:
                    continue
                all_uris.append((uri, file_path))
        
        all_uris.sort(key=lambda x: x[0])
        
        for uri, file_path in all_uris[:batch_size]:
            stat = file_path.stat()
            batch.append({
                "uri": uri,
                "size": stat.st_size,
                "last_modified": datetime.fromtimestamp(stat.st_mtime),
                "etag": str(int(stat.st_mtime))
            })
            
        return batch
