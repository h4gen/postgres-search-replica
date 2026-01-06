import asyncio
import random
import logging
import uuid
from pg_replica.config import Settings
from pg_replica.database import init_pools, get_source_conn, close_pools

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def live_gen():
    settings = Settings()
    await init_pools(settings)
    
    table_name = "production_docs"
    logger.info(f"Starting Live Data Generator for {table_name}...")
    
    try:
        while True:
            action = random.choice(["insert", "update", "delete"])
            
            async with await get_source_conn() as conn:
                async with conn.cursor() as cur:
                    if action == "insert":
                        doc_id = str(uuid.uuid4())
                        title = f"Live Doc {doc_id[:8]}"
                        content = f"Continuously generated content for testing. Seed: {random.random()}"
                        await cur.execute(
                            f"INSERT INTO {table_name} (id, title, content) VALUES (%s, %s, %s)",
                            (doc_id, title, content)
                        )
                        logger.info(f"Inserted doc {doc_id}")
                    
                    elif action == "update":
                        await cur.execute(f"SELECT id FROM {table_name} ORDER BY random() LIMIT 1")
                        row = await cur.fetchone()
                        if row:
                            doc_id = row[0]
                            await cur.execute(
                                f"UPDATE {table_name} SET content = %s WHERE id = %s",
                                (f"Updated at {random.random()}", doc_id)
                            )
                            logger.info(f"Updated doc {doc_id}")

                    elif action == "delete":
                        await cur.execute(f"SELECT id FROM {table_name} ORDER BY random() LIMIT 1")
                        row = await cur.fetchone()
                        if row:
                            doc_id = row[0]
                            await cur.execute(f"DELETE FROM {table_name} WHERE id = %s", (doc_id,))
                            logger.info(f"Deleted doc {doc_id}")
                
                await conn.commit()
            
            # Wait between cycles to simulate realistic traffic
            await asyncio.sleep(random.uniform(1.0, 5.0))
            
    except asyncio.CancelledError:
        logger.info("Live generator stopping...")
    finally:
        await close_pools()

if __name__ == "__main__":
    try:
        asyncio.run(live_gen())
    except KeyboardInterrupt:
        pass
