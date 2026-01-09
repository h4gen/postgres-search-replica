import abc
import logging
from typing import Any, Dict, List, Optional
from .config import SearchPipeline, Settings
from .database import dict_row

logger = logging.getLogger(__name__)

class SearchStrategy(abc.ABC):
    @abc.abstractmethod
    async def search(
        self, 
        query: str, 
        embedding: List[float], 
        limit: int, 
        config: SearchPipeline,
        conn_provider: Any,
        target_name: str,
        settings: Settings
    ) -> List[Dict[str, Any]]:
        """Execute search using the specific engine."""
        pass


class PostgresSearchStrategy(SearchStrategy):
    async def search(
        self, 
        query: str, 
        embedding: List[float], 
        limit: int, 
        config: SearchPipeline,
        conn_provider: Any,
        target_name: str,
        settings: Settings
    ) -> List[Dict[str, Any]]:
        replica_table = f"{target_name}_search"
        conn = await conn_provider()
        
        async with conn.cursor(row_factory=dict_row) as cur:
            if config.storage.postgres.profile == "hybrid":
                # HYBRID SEARCH (RRF): Vector + Full-Text
                sql = f"""
                    WITH ranked AS (
                        SELECT 
                            {config.ingest.p_key}, 
                            chunk,
                            row_number() OVER (ORDER BY embedding <=> %s) as vector_rank,
                            row_number() OVER (ORDER BY ts_rank(ts_col, websearch_to_tsquery('english', %s)) DESC) as text_rank
                        FROM {replica_table}
                    )
                    SELECT 
                        {config.ingest.p_key}, 
                        chunk,
                        (1.0 / (60 + vector_rank) + 1.0 / (60 + text_rank)) as score
                    FROM ranked
                    ORDER BY score DESC
                    LIMIT %s
                """
                logger.debug(f"Executing Postgres Hybrid RRF search on {replica_table}")
                await cur.execute(sql, (embedding, query, limit))
            else:
                # VECTOR SEARCH ONLY
                sql = f"""
                    SELECT 
                        {config.ingest.p_key}, 
                        chunk,
                        embedding <=> %s::vector as distance
                    FROM {replica_table}
                    ORDER BY distance ASC
                    LIMIT %s
                """
                logger.debug(f"Executing Postgres Vector search on {replica_table}")
                await cur.execute(sql, (embedding, limit))

            rows = await cur.fetchall()
            results = []
            for row in rows:
                item = {
                    "id": row[config.ingest.p_key],
                    "content": row["chunk"],
                }
                if "score" in row:
                    item["score"] = float(row["score"])
                else:
                    item["distance"] = float(row["distance"])
                results.append(item)
            return results

class QdrantSearchStrategy(SearchStrategy):
    async def search(
        self, 
        query: str, 
        embedding: List[float], 
        limit: int, 
        config: SearchPipeline,
        conn_provider: Any,
        target_name: str,
        settings: Settings
    ) -> List[Dict[str, Any]]:
        # Find mirror config for Qdrant
        # Resolve mirror IDs from config.storage.mirrors using settings.mirrors
        mirror = None
        for m_id in config.storage.mirrors:
            if m_id in settings.mirrors and settings.mirrors[m_id].type == "qdrant":
                mirror = settings.mirrors[m_id]
                break
        
        if not mirror:
            raise ValueError(f"No Qdrant mirror configured for table '{config.ingest.table}'")
        
        from qdrant_client import QdrantClient
        from qdrant_client.http import models
        
        url = mirror.config.get("url")
        prefix = mirror.config.get("prefix", "")
        
        # In a real app, we might want to pool clients
        client = QdrantClient(url)
        collection_name = f"{prefix}{config.ingest.table}_production"
        version_name = f"{prefix}{config.ingest.table}_{config.get_version_id()}"
        
        # Check if alias exists, otherwise use version name
        search_target = version_name
        try:
            collections = client.get_collections().collections
            # If production alias exists, use it
            if any(c.name == collection_name for c in collections):
                search_target = collection_name
        except Exception:
            pass
            
        logger.debug(f"Executing Qdrant search on {search_target}")
        
        try:
            res = client.query_points(
                collection_name=search_target,
                query=embedding,
                limit=limit,
                with_payload=True
            ).points
        except Exception as e:
            logger.error(f"Qdrant search failed on {search_target}: {e}")
            raise
        
        results = []
        for hit in res:
            results.append({
                "id": hit.id,
                "content": hit.payload.get("chunk") or hit.payload.get("content"),
                "distance": hit.score, 
                "engine": "qdrant"
            })
        return results

