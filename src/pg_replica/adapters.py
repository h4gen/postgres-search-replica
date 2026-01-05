import abc
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from qdrant_client import QdrantClient
from qdrant_client.http import models

logger = logging.getLogger(__name__)

@dataclass
class OutboxEntry:
    id: int
    target_name: str
    version_id: str
    source_id: str
    action: str
    payload: Optional[Dict[str, Any]]

class SinkAdapter(abc.ABC):
    @abc.abstractmethod
    async def sync_batch(self, entries: List[OutboxEntry]):
        """Sync a batch of entries to the destination."""
        pass

    @abc.abstractmethod
    async def update_alias(self, target_name: str, version_id: str, vector_size: int = 768):
        """Update the 'production' alias to point to the specific versioned index."""
        pass

class QdrantSinkAdapter(SinkAdapter):
    def __init__(self, url: str, collection_prefix: str = ""):
        self.client = QdrantClient(url)
        self.collection_prefix = collection_prefix

    def _get_collection_name(self, target_name: str, version_id: str) -> str:
        return f"{self.collection_prefix}{target_name}_{version_id}"

    async def _ensure_collection(self, collection_name: str, vector_size: int):
        # Note: In a real app, we might want to cache this or handle it more robustly
        try:
            self.client.get_collection(collection_name)
        except Exception:
            logger.info(f"Creating Qdrant collection: {collection_name}")
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(
                    size=vector_size,
                    distance=models.Distance.COSINE
                )
            )

    async def sync_batch(self, entries: List[OutboxEntry]):
        if not entries:
            return

        # Group by collection (target + version)
        groups: Dict[str, List[OutboxEntry]] = {}
        for entry in entries:
            key = self._get_collection_name(entry.target_name, entry.version_id)
            if key not in groups:
                groups[key] = []
            groups[key].append(entry)

        for coll_name, batch in groups.items():
            upserts = []
            deletes = []
            
            vector_size = 0
            
            for entry in batch:
                if entry.action == "UPSERT" and entry.payload:
                    embedding_str = entry.payload.get("embedding")
                    if embedding_str:
                        # Convert text back to list
                        import json
                        embedding = json.loads(embedding_str)
                        vector_size = len(embedding)
                        
                        # Qdrant IDs must be unsigned int or UUID
                        try:
                            p_id = int(entry.source_id)
                        except ValueError:
                            p_id = entry.source_id

                        upserts.append(
                            models.PointStruct(
                                id=p_id,
                                vector=embedding,
                                payload={
                                    "content": entry.payload.get("content") or entry.payload.get("description"),
                                    "target_name": entry.target_name,
                                    "version_id": entry.version_id
                                }
                            )
                        )
                elif entry.action == "DELETE":
                    try:
                        p_id = int(entry.source_id)
                    except ValueError:
                        p_id = entry.source_id
                    deletes.append(p_id)

            if upserts:
                await self._ensure_collection(coll_name, vector_size)
                self.client.upsert(
                    collection_name=coll_name,
                    points=upserts
                )
            
            if deletes:
                self.client.delete(
                    collection_name=coll_name,
                    points_selector=models.PointIdsList(
                        points=deletes
                    )
                )

        logger.info(f"Synced {len(entries)} entries to Qdrant")

    async def update_alias(self, target_name: str, version_id: str, vector_size: int = 768):
        alias_name = f"{self.collection_prefix}{target_name}_production"
        collection_name = self._get_collection_name(target_name, version_id)
        
        logger.info(f"Updating Qdrant alias {alias_name} -> {collection_name}")
        
        # Ensure collection exists before creating alias
        await self._ensure_collection(collection_name, vector_size)
        
        try:
            # Qdrant aliases are updated via operations. 
            # We use an atomic swap: Delete (if exists) + Create.
            
            self.client.update_collection_aliases(
                change_aliases_operations=[
                    models.DeleteAliasOperation(
                        delete_alias=models.DeleteAlias(
                            alias_name=alias_name
                        )
                    ),
                    models.CreateAliasOperation(
                        create_alias=models.CreateAlias(
                            collection_name=collection_name,
                            alias_name=alias_name
                        )
                    )
                ]
            )
            logger.info(f"Successfully updated Qdrant alias {alias_name}")
        except Exception as e:
            # If Delete failed because it didn't exist, try just Create
            if "not found" in str(e).lower():
                try:
                    self.client.update_collection_aliases(
                        change_aliases_operations=[
                            models.CreateAliasOperation(
                                create_alias=models.CreateAlias(
                                    collection_name=collection_name,
                                    alias_name=alias_name
                                )
                            )
                        ]
                    )
                    logger.info(f"Successfully created initial Qdrant alias {alias_name}")
                    return
                except Exception as inner_e:
                    logger.error(f"Failed to create initial Qdrant alias {alias_name}: {inner_e}")
                    raise inner_e
            logger.error(f"Failed to update Qdrant alias {alias_name}: {e}")
            raise e
