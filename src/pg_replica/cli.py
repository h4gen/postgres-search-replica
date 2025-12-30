import argparse
import asyncio
import logging
import signal
import sys
from .client import PGSearchReplica

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def run_service():
    """Run the PGSearchReplica as a long-running service."""
    # Settings are automatically loaded from env vars by Pydantic
    replica = PGSearchReplica(sync=True)
    
    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    
    stop_event = asyncio.Event()
    
    def handle_exit():
        logger.info("Shutdown signal received...")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_exit)

    try:
        # Start the replica (this starts the orchestrator)
        await replica.start(sync=True)
        logger.info("PGSearchReplica service started. Press Ctrl+C to stop.")
        
        # Wait until we receive a stop signal
        await stop_event.wait()
    except Exception as e:
        logger.error(f"Service failed: {e}")
        sys.exit(1)
    finally:
        await replica.stop()
        logger.info("Service stopped.")


def main():
    parser = argparse.ArgumentParser(description="PGSearchReplica CLI")
    subparsers = parser.add_subparsers(dest="command")

    # Start command
    start_parser = subparsers.add_parser("start", help="Start the replication service")
    
    args = parser.parse_args()

    if args.command == "start":
        try:
            asyncio.run(run_service())
        except KeyboardInterrupt:
            pass
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

