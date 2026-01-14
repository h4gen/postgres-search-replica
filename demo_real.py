import asyncio
import os
import logging
from rich.console import Console
from rich.progress import Progress
import psycopg

# Ensure we can import src
import sys
import os
sys.path.insert(0, os.path.join(os.getcwd(), "src"))

from pg_replica.client import PGSearchReplica
import httpx

# UI Console
console = Console(stderr=True)

# Note: We NO LONGER hijack the console. PGSearchReplica takes it as an argument.

DB_DSN = "postgresql://postgres:password@localhost:5433/production_db"
SINK_DSN = "postgresql://postgres:password@localhost:5434/search_replica_db"

def setup_source_data():
    """Ensure the source table exists and has data."""
    console.print("[bold blue]Setting up Source Data in Postgres...[/bold blue]")
    try:
        with psycopg.connect(DB_DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                # 0. Clean up previous run state
                
                # Robust Slot Drop with Retry Loop
                slot_name = "sub_products_real"
                max_retries = 10
                for i in range(max_retries):
                    # 1. Kill Owner
                    cur.execute("SELECT active_pid FROM pg_replication_slots WHERE slot_name = %s", (slot_name,))
                    row = cur.fetchone()
                    if row and row[0]:
                        pid = row[0]
                        console.print(f"[yellow]Terminating active backend {pid} for slot {slot_name} (Attempt {i+1})...[/yellow]")
                        cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                        import time
                        time.sleep(0.5)

                    # 2. Try Drop
                    try:
                        cur.execute("SELECT pg_drop_replication_slot(%s) FROM pg_replication_slots WHERE slot_name = %s;", (slot_name, slot_name))
                        # Check if gone
                        cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (slot_name,))
                        if not cur.fetchone():
                            break # Success
                    except Exception as e:
                        console.print(f"[dim]Drop failed: {e}. Retrying...[/dim]")
                        time.sleep(0.5)

                cur.execute("DROP TABLE IF EXISTS products_real CASCADE;")
                cur.execute("DROP PUBLICATION IF EXISTS pub_products_real;")
                
                # 1. Create Table
                cur.execute("""
                    CREATE TABLE products_real (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        description TEXT
                    );
                """)
                # 2. Add WAL Level (Should be set globally, but ensuring replica identity)
                cur.execute("ALTER TABLE products_real REPLICA IDENTITY FULL;")
                
                # 3. Seed Data
                data = [
                    (1, "Red Sneakers", "Comfortable red sneakers for daily jogging."),
                    (2, "Blue Running Shoes", "High performance blue shoes for marathons."),
                    (3, "Green T-Shirt", "A casual green t-shirt made of cotton."),
                    (4, "Red T-Shirt", "Bright red t-shirt, matches the sneakers."),
                    (5, "Leather Boots", "Rugged brown leather boots for hiking.")
                ]
                for pid, name, desc in data:
                    cur.execute("""
                        INSERT INTO products_real (id, name, description) 
                        VALUES (%s, %s, %s)
                        ON CONFLICT (id) DO UPDATE 
                        SET name = EXCLUDED.name, description = EXCLUDED.description;
                    """, (pid, name, desc))

        console.print("[bold green]✓ Source Data Ready (5 rows)[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Failed to setup source: {e}[/bold red]")
        sys.exit(1)

async def clean_sink_db():
    """Drop and recreate the sink database to ensure no stale state/config."""
    console.print("[bold blue]Cleaning Sink Database...[/bold blue]")
    try:
        import psycopg
        # 1. Drop subscriptions inside the DB first (locks DB)
        try:
            async with await psycopg.AsyncConnection.connect(SINK_DSN, autocommit=True) as conn:
                async with conn.cursor() as cur:
                    # Find all subscriptions
                    await cur.execute("SELECT subname FROM pg_subscription")
                    subs = [row[0] async for row in cur]
                    for sub in subs:
                        try:
                            console.print(f"Dropping subscription {sub}...")
                            await cur.execute(f"ALTER SUBSCRIPTION {sub} DISABLE")
                            await cur.execute(f"ALTER SUBSCRIPTION {sub} SET (slot_name = NONE)") # Release slot on source
                            await cur.execute(f"DROP SUBSCRIPTION {sub}")
                        except Exception as e:
                            console.print(f"[yellow]Failed to drop sub {sub}: {e}[/yellow]")
        except psycopg.OperationalError:
            # DB might not exist
            pass

        # 2. Drop Database from 'postgres'
        dsn = "postgresql://postgres:password@localhost:5434/postgres"
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            async with conn.cursor() as cur:
                # Terminate connections to search_replica_db
                await cur.execute("""
                    SELECT pg_terminate_backend(pid) 
                    FROM pg_stat_activity 
                    WHERE datname = 'search_replica_db' AND pid <> pg_backend_pid()
                """)
                await cur.execute("DROP DATABASE IF EXISTS search_replica_db")
                await cur.execute("CREATE DATABASE search_replica_db")
        console.print("[green]✓ Sink Database Cleaned[/green]")
    except Exception as e:
        console.print(f"[bold red]Failed to clean sink: {e}[/bold red]")
        # Don't exit, might be just connection error, try to proceed


async def prepull_models():
    """Ensure Ollama models are pulled to avoid timeouts during vectorizer creation."""
    console.print("[bold blue]Pre-pulling Ollama models...[/bold blue]")
    models = ["nomic-embed-text", "all-minilm"]
    import httpx
    async with httpx.AsyncClient() as client:
        for model in models:
            console.print(f"Checking {model}...")
            # We use the pull endpoint
            try:
                await client.post("http://localhost:11434/api/pull", json={"model": model, "stream": False}, timeout=120.0)
                console.print(f"[green]✓ {model} ready[/green]")
            except Exception as e:
                console.print(f"[red]Failed to pull {model}: {e}[/red]")

async def main():
    # 1. Clean Slate (Seeding Rule: Seed BEFORE calling PGSearchReplica)
    await clean_sink_db()
    setup_source_data()
    await prepull_models()
    

    try:
        # Configuration for Real Pipeline
        pipeline_config = {
            "ingest": {
                "table": "products_real", 
                "columns": ["id", "name", "description"]
            },
            "pipeline": {
                "template": "Name: $name\nDescription: $description\n\n$chunk",
                "content_column": "description",
                "embedding": {
                    "provider": "ollama", 
                    "model": "nomic-embed-text", 
                    "dimension": 768
                }
            }
        }

        # CRITICAL: Tell the Sink Container how to reach the Source Container
        os.environ["SUBSCRIPTION_SOURCE_URL"] = "postgresql://postgres:password@dev-source-1:5432/production_db"

        # 3. Enter Glass Cockpit
        replica = PGSearchReplica(
            sync=True, 
            verbose=False,
            console=console,
            pipelines={"products_real": pipeline_config},
            source_url=DB_DSN,
            sink_url="postgresql://postgres:password@localhost:5434/search_replica_db"
        )
        try:
            console.print("\n[bold]--- 1. Initial Sync ---[/bold]")
            await replica.start()

            # wait() now supports instance console and better progress
            await replica.products_real.wait()
            
            # Show Glass Cockpit Status Box
            await replica.products_real.show()

            # 4. SearchOps: Branching
            console.print("\n[bold]--- 2. SearchOps: Branching ---[/bold]")
            console.print("Creating experiment branch 'v2' with model 'all-minilm'...")
            
            # branch() returns a ChangeSet which shows a Git-style diff
            branch_plan = await replica.products_real.branch("v2", model="all-minilm", dimension=384)
            console.print(branch_plan) 
            
            await branch_plan.apply()
            await replica.products_real.wait() # Wait for everything to be ready
            
            # 5. Compare
            console.print("\n[bold]--- 3. Side-by-Side Comparison ---[/bold]")
            query = "red shoes"
            await replica.products_real.compare("products_real", "v2", query)

            # 6. Atomic Promotion
            console.print("\n[bold]--- 4. Atomic Promotion ---[/bold]")
            console.print("Experiment v2 is the winner! Promoting to live...")
            
            # promote() merges the branch config into the main pipeline
            promo_plan = await replica.products_real.promote("v2")
            console.print(promo_plan)
            
            await promo_plan.apply()
            await replica.products_real.wait()
            
            console.print("[bold green]✓ Pipeline Promoted to v2![/bold green]")
            await replica.products_real.show()
            
        finally:
            await replica.stop()
    finally:
        # No cleanup of server process needed
        pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
