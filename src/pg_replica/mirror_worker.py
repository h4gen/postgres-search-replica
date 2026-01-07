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
        for target_name, config in self.settings.pipelines.items():
            for mirror_cfg in config.storage.mirrors:
                # MirrorConfig is now an object, we need to convert to dict for legacy adapter code
                # or update _process_mirror to handle objects.
                # Let's check _process_mirror signature: it expects Dict[str, Any].
                # So we verify if config.storage.mirrors is List[MirrorConfig] or List[Dict].
                # It is List[MirrorConfig]. So we should use mirror_cfg.model_dump().
                await self._process_mirror(target_name, mirror_cfg.model_dump())

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
                if rows:
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
                    logger.info(f"MirrorWorker for {mirror_id}/{target_name} found {len(rows)} entries (last_id={last_id})")
                    import time
                    start_time = time.time()
                    try:
                        await adapter.sync_batch(entries)
                        latency_ms = int((time.time() - start_time) * 1000)
                        
                        # Update registry for sync progress + latency
                        new_last_id = entries[-1].id
                        await cur.execute(
                            """
                            INSERT INTO _sink_mirror_registry (mirror_id, target_name, last_processed_id, last_sync_latency_ms, error_count, updated_at)
                            VALUES (%s, %s, %s, %s, 0, NOW())
                            ON CONFLICT (mirror_id, target_name) 
                            DO UPDATE SET 
                                last_processed_id = EXCLUDED.last_processed_id, 
                                last_sync_latency_ms = EXCLUDED.last_sync_latency_ms,
                                error_count = 0,
                                updated_at = NOW()
                            """,
                            (mirror_id, target_name, new_last_id, latency_ms)
                        )
                        await conn.commit()
                    except Exception as e:
                        await conn.rollback()
                        error_msg = str(e)
                        logger.error(f"Failed to sync batch to mirror {mirror_id}: {error_msg}")
                        # Record error in registry
                        await cur.execute(
                            """
                            INSERT INTO _sink_mirror_registry (mirror_id, target_name, error_count, last_error, updated_at)
                            VALUES (%s, %s, 1, %s, NOW())
                            ON CONFLICT (mirror_id, target_name) 
                            DO UPDATE SET 
                                error_count = _sink_mirror_registry.error_count + 1,
                                last_error = EXCLUDED.last_error,
                                updated_at = NOW()
                            """,
                            (mirror_id, target_name, error_msg)
                        )
                        await conn.commit()
                        return # Stop processing this mirror if sync fails

                # 4. Check for Version Promotion in Postgres (even if no new rows)
                try:
                    sub_name = f"sub_{target_name}"
                    logger.debug(f"MirrorWorker checking promotion for {sub_name}...")
                    await cur.execute(
                        "SELECT config_hash FROM _replica_state WHERE key = %s",
                        (sub_name,)
                    )
                    state_row = await cur.fetchone()
                    if state_row and state_row[0]:
                        current_hash = state_row[0]
                        promoted_version = current_hash[:8]
                        logger.debug(f"Found promoted hash {current_hash} (version {promoted_version}) for {sub_name}")
                        
                        await cur.execute(
                            "SELECT promoted_version_id FROM _sink_mirror_registry WHERE mirror_id = %s AND target_name = %s",
                            (mirror_id, target_name)
                        )
                        reg_row = await cur.fetchone()
                        last_mirrored_version = reg_row[0] if reg_row else None
                        
                        logger.debug(f"Mirror {mirror_id} last promoted version: {last_mirrored_version}")

                        if promoted_version != last_mirrored_version:
                            logger.info(f"Promoting mirror {mirror_id} for {target_name} to version {promoted_version}...")
                            
                            # Use configured dimension to avoid 404s if collection doesn't exist yet
                            # Use configured dimension to avoid 404s if collection doesn't exist yet
                            config = self.settings.pipelines.get(target_name)
                            vector_size = config.pipeline.embedding.dimension if config else 768
                            
                            await adapter.update_alias(target_name, promoted_version, vector_size=vector_size)
                            await cur.execute(
                                """
                                INSERT INTO _sink_mirror_registry (mirror_id, target_name, promoted_version_id, updated_at)
                                VALUES (%s, %s, %s, NOW())
                                ON CONFLICT (mirror_id, target_name) 
                                DO UPDATE SET promoted_version_id = EXCLUDED.promoted_version_id, updated_at = NOW()
                                """,
                                (mirror_id, target_name, promoted_version)
                            )
                            await conn.commit()
                    else:
                        logger.debug(f"No replica state found for key {sub_name}")
                except Exception as e:
                    await conn.rollback()
                    # Catch 'column does not exist' gracefully during initial startup migration
                    if "promoted_version_id" in str(e):
                        logger.debug("Mirror registry migration still pending...")
                    else:
                        logger.error(f"Failed to update alias for mirror {mirror_id}: {e}")

    def stop(self):
        self._shutdown = True
