import asyncio
import logging
import json
from typing import List, Dict, Any
from .config import Settings, TableConfig
from .database import get_sink_conn
from .adapters import OutboxEntry, QdrantSinkAdapter, SinkAdapter

logger = logging.getLogger(__name__)

class MirrorWorker:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.adapters: Dict[str, SinkAdapter] = {}
        self._shutdown = False

    def _get_adapter(self, mirror_cfg: Dict[str, Any]) -> SinkAdapter:
        m_type = mirror_cfg.get("type")
        m_url = mirror_cfg.get("url")
        m_id = mirror_cfg.get("id")
        
        cache_key = f"{m_type}_{m_url}_{m_id}"
        if cache_key not in self.adapters:
            if m_type == "qdrant":
                self.adapters[cache_key] = QdrantSinkAdapter(m_url, collection_prefix=mirror_cfg.get("prefix", ""))
            else:
                raise ValueError(f"Unsupported mirror type: {m_type}")
        return self.adapters[cache_key]

    async def run(self):
        logger.info("Starting Mirror Worker...")
        while not self._shutdown:
            try:
                await self._process_all_mirrors()
            except Exception as e:
                logger.error(f"Error in mirror worker loop: {e}", exc_info=True)
            
            await asyncio.sleep(2) # Poll interval

    async def _process_all_mirrors(self):
        # We process mirrors defined in ALL table configs
        for target_name, config in self.settings.tables.items():
            for mirror_cfg in config.mirrors:
                await self._process_mirror(target_name, mirror_cfg)

    async def _process_mirror(self, target_name: str, mirror_cfg: Dict[str, Any]):
        mirror_id = mirror_cfg.get("id")
        adapter = self._get_adapter(mirror_cfg)
        
        async with await get_sink_conn() as conn:
            async with conn.cursor() as cur:
                # 1. Get last processed ID for this mirror
                await cur.execute(
                    "SELECT last_processed_id FROM _sink_mirror_registry WHERE mirror_id = %s AND target_name = %s",
                    (mirror_id, target_name)
                )
                row = await cur.fetchone()
                last_id = row[0] if row else 0

                # 2. Fetch new outbox entries
                await cur.execute(
                    """
                    SELECT id, target_name, version_id, source_id, action, payload 
                    FROM _sink_outbox 
                    WHERE target_name = %s AND id > %s 
                    ORDER BY id ASC 
                    LIMIT %s
                    """,
                    (target_name, last_id, 100) # Batch size
                )
                rows = await cur.fetchall()
                if not rows:
                    return

                entries = [
                    OutboxEntry(
                        id=r[0],
                        target_name=r[1],
                        version_id=r[2],
                        source_id=r[3],
                        action=r[4],
                        payload=r[5]
                    ) for r in rows
                ]

                # 3. Dispatch to adapter
                try:
                    await adapter.sync_batch(entries)
                    
                    # 4. Update registry
                    new_last_id = entries[-1].id
                    await cur.execute(
                        """
                        INSERT INTO _sink_mirror_registry (mirror_id, target_name, last_processed_id, updated_at)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (mirror_id, target_name) 
                        DO UPDATE SET last_processed_id = EXCLUDED.last_processed_id, updated_at = NOW()
                        """,
                        (mirror_id, target_name, new_last_id)
                    )
                    await conn.commit()
                except Exception as e:
                    await conn.rollback()
                    logger.error(f"Failed to sync batch to mirror {mirror_id}: {e}")

    def stop(self):
        self._shutdown = True
