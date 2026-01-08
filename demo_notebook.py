
import asyncio
import logging
from pg_replica import connect

# Configure logging to see the output
logging.basicConfig(level=logging.INFO)

async def run_demo():
    print("🚀 Starting God Tier DX Demo...")
    
    # 1. Connect (Mocking the DB connection for this script if needed, 
    # but let's assume the client initializes correctly)
    # We use a dummy URL since we might not have a real DB running in this specific interaction
    client = connect(sink_url="postgresql://user:pass@localhost:5432/db", sync=False)
    
    print("✅ Connected.")
    
    # 2. Fluent Access
    products = client.products
    print(f"✅ Accessed pipeline: {products.name}")
    
    # 3. Configure (Plan)
    print("\n--- Planning Configuration ---")
    changeset = await products.configure(
        model="openai/small",
        columns=["name", "description"],
        template="Product: $name\nContext: $chunk"
    )
    print(changeset)
    
    # 4. Apply (Simulated)
    print("\n--- Applying Configuration ---")
    await changeset.apply()
    print("✅ Configuration applied.")
    
    # 5. Branching (SearchOps)
    print("\n--- Creating Branch ---")
    # interactive=False to avoid waiting for sync in this quick test
    branch = await products.branch("experiment-v2", model="voyage/large", interactive=False)
    print(f"✅ Branch created: {branch.name}")
    
    # 6. Promotion
    print("\n--- Promoting Branch ---")
    await products.promote("experiment-v2")
    print("✅ Promotion triggered.")

if __name__ == "__main__":
    asyncio.run(run_demo())
