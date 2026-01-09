import logging
from typing import AsyncGenerator, Dict, Any, List
from contextlib import asynccontextmanager
import boto3
from .base import BaseSourceAdapter
from ..config import S3SourceConfig

logger = logging.getLogger(__name__)

class S3SourceAdapter(BaseSourceAdapter):
    def __init__(self, name: str, config: S3SourceConfig):
        self.name = name
        self.config = config
        self.s3 = None

    async def connect(self) -> None:
        self.s3 = boto3.client(
            "s3",
            region_name=self.config.region,
            endpoint_url=self.config.endpoint_url
        )
        # Check accessibility
        try:
            self.s3.head_bucket(Bucket=self.config.bucket)
        except Exception as e:
            logger.error(f"Failed to connect to S3 bucket '{self.config.bucket}': {e}")
            raise

    async def close(self) -> None:
        pass

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[Any, None]:
        yield self.s3

    async def discovery_state(self) -> dict:
        if not self.s3:
            return {"is_reachable": False, "error": "Not connected"}
        try:
            resp = self.s3.list_objects_v2(
                Bucket=self.config.bucket,
                Prefix=self.config.prefix,
                MaxKeys=1
            )
            return {
                "is_reachable": True,
                "bucket": self.config.bucket,
                "prefix": self.config.prefix,
                "has_objects": "Contents" in resp
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
        if not self.s3:
             raise RuntimeError("S3 adapter not connected")
             
        batch = []
        # last_id is the last URI processed
        start_after = ""
        if last_id and last_id.startswith(f"s3://{self.config.bucket}/"):
            start_after = last_id.replace(f"s3://{self.config.bucket}/", "", 1)
        
        # Note: list_objects_v2 is blocking, but for simplicity we keep it. 
        # In a real async orchestrator, we might use aiobotocore.
        resp = self.s3.list_objects_v2(
            Bucket=self.config.bucket,
            Prefix=self.config.prefix,
            StartAfter=start_after,
            MaxKeys=batch_size
        )
        
        if "Contents" in resp:
            for obj in resp["Contents"]:
                # skip directory markers
                if obj["Key"].endswith("/"):
                    continue
                    
                uri = f"s3://{self.config.bucket}/{obj['Key']}"
                batch.append({
                    "uri": uri,
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"],
                    "etag": obj["ETag"].strip('"')
                })
                
        return batch
