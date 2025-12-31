import asyncio
from pg_replica.database import init_pools, get_sink_conn, close_pools
from pg_replica.config import settings

async def run():
    await init_pools(settings)
    try:
        async with await get_sink_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT p.proname, pg_get_function_arguments(p.oid) 
                    FROM pg_proc p 
                    JOIN pg_namespace n ON p.pronamespace = n.oid 
                    WHERE n.nspname = 'ai' AND p.proname = 'create_vectorizer'
                """)
                print("create_vectorizer args:", await cur.fetchall())
                
                await cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                print("Tables:", [r[0] for r in await cur.fetchall()])
    finally:
        await close_pools()

if __name__ == "__main__":
    asyncio.run(run())

