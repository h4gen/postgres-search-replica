import abc
import logging
from typing import Any, Dict, List, Optional
from .config import ReplicaConfig
from .database import dict_row

logger = logging.getLogger(__name__)

class SearchStrategy(abc.ABC):
    @abc.abstractmethod
    async def search(
        self, 
        query: str, 
        embedding: List[float], 
        limit: int, 
        target_name: str,
        config: ReplicaConfig,
        conn_provider: Any
    ) -> List[Dict[str, Any]]:
        """Execute search using the specific engine."""
        pass


class PostgresSearchStrategy(SearchStrategy):
    async def search(
        self, 
        query: str, 
        embedding: List[float], 
        limit: int, 
        target_name: str,
        config: ReplicaConfig,
        conn_provider: Any
    ) -> List[Dict[str, Any]]:
        replica_table = f"{target_name}_search"
        conn = await conn_provider()
        
        async with conn.cursor(row_factory=dict_row) as cur:
            if config.search.profile == "hybrid":
                # HYBRID SEARCH (RRF): Vector + Full-Text
                sql = f"""
                    WITH ranked AS (
                        SELECT 
                            {config.source.primary_key}, 
                            {config.formatting.target_content_column},
                            row_number() OVER (ORDER BY {config.search.embedding_column} <=> %s) as vector_rank,
                            row_number() OVER (ORDER BY ts_rank(ts_col, websearch_to_tsquery('english', %s)) DESC) as text_rank
                        FROM {replica_table}
                    )
                    SELECT 
                        {config.source.primary_key}, 
                        {config.formatting.target_content_column},
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
                        {config.source.primary_key}, 
                        {config.formatting.target_content_column},
                        {config.search.embedding_column} <=> %s::vector as distance
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
                    "id": row[config.source.primary_key],
                    "content": row[config.formatting.target_content_column],
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
        target_name: str,
        config: ReplicaConfig,
        conn_provider: Any
    ) -> List[Dict[str, Any]]:
        # Find mirror config for Qdrant
        mirror = next((m for m in config.mirrors.targets if m.type == "qdrant"), None)
        if not mirror:
            raise ValueError(f"No Qdrant mirror configured for table '{config.source.table}'")
        
        from qdrant_client import AsyncQdrantClient
        from qdrant_client.http import models
        
        url = mirror.url
        prefix = mirror.prefix
        
        # Use AsyncQdrantClient
        client = AsyncQdrantClient(url)
        try:
            # Standardize naming: prefix + target_name + _production
            collection_name = f"{prefix}{target_name}_production"
            version_name = f"{prefix}{target_name}_{config.get_version_id()}"
            
            # Check if alias exists, otherwise use version name
            search_target = version_name
            try:
                aliases = await client.get_aliases()
                # If production alias exists, use it
                if any(a.alias_name == collection_name for a in aliases.aliases):
                    search_target = collection_name
            except Exception as e:
                logger.warning(f"Failed to check aliases: {e}")
                pass
                
            logger.debug(f"Executing Qdrant search on {search_target}")
            
            try:
                res = await client.query_points(
                    collection_name=search_target,
                    query=embedding,
                    limit=limit,
                    with_payload=True
                )
                hits = res.points
                logger.info(f"Qdrant search on {search_target} returned {len(hits)} hits")
            except Exception as e:
                logger.error(f"Qdrant search failed on {search_target}: {e}")
                raise
            
            results = []
            for hit in hits:
                results.append({
                    "id": hit.id,
                    "content": hit.payload.get("content") or hit.payload.get(config.formatting.target_content_column),
                    "distance": hit.score, 
                    "engine": "qdrant"
                })
            return results
        finally:
            await client.close()
