import asyncio
import logging
import subprocess
from datetime import timedelta
from typing import Optional

from .config import Settings
from .database import (
    setup_source,
    setup_sink,
    drop_subscription_completely,
    check_and_protect_source,
    connect_db,
    init_pools,
    close_pools,
)
from pgai.vectorizer.worker import Worker

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._pg_process: Optional[subprocess.Popen] = None
        self._tasks: list[asyncio.Task] = []
        self._stop_event = asyncio.Event()

    async def _is_port_open(self, port: int) -> bool:
        try:
            _, writer = await asyncio.open_connection("localhost", port)
            writer.close()
            await writer.wait_closed()
            return True
        except Exception:
            return False

    async def _wait_for_pg(self, port: int, timeout: int = 30):
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < timeout:
            if await self._is_port_open(port):
                # Extra check to ensure we can actually connect via psycopg
                try:
                    async with await connect_db(
                        self.settings.resolved_sink_url
                    ) as conn:
                        return True
                except Exception:
                    pass
            await asyncio.sleep(1)
        return False

    def _start_local_postgres(self):
        """Starts a local Postgres process if sink_url is 'local'."""
        if self.settings.sink_url != "local":
            return

        data_dir = self.settings.data_dir
        if not data_dir.exists():
            logger.info(
                f"Initializing new Postgres data directory at {data_dir}..."
            )
            data_dir.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                ["initdb", "-D", str(data_dir)], check=True, capture_output=True
            )
            # Allow all connections for dev/container usage
            with open(data_dir / "pg_hba.conf", "a") as f:
                f.write("\nhost all all all trust\n")

        port = self.settings.local_port
        logger.info(f"Starting local Postgres on port {port}...")

        # We use a custom unix socket directory to avoid collisions
        run_dir = self.settings.base_dir / "run"
        run_dir.mkdir(parents=True, exist_ok=True)

        self._pg_process = subprocess.Popen(
            [
                "postgres",
                "-D",
                str(data_dir),
                "-p",
                str(port),
                "-k",
                str(run_dir),
                # Ensure we have enough connections and extensions can load
                "-c",
                "max_connections=100",
                "-c",
                "shared_preload_libraries=vector",
                "-c",
                "listen_addresses=*",
            ]
        )

    async def _log_pgai_status(self):
        """Poll pgai status and log progress."""
        try:
            async with await connect_db(
                self.settings.resolved_sink_url
            ) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT source_table, pending_items FROM ai.vectorizer_status"
                    )
                    results = await cur.fetchall()
                    for table, pending in results:
                        logger.info(
                            f"pgai Status for {table}: {pending} items pending"
                        )
        except Exception as e:
            logger.debug(f"Failed to fetch pgai status: {e}")

    async def _replication_loop(self):
        """Main loop for the replicator daemon logic."""
        logger.info("Starting replication watchdog...")
        while not self._stop_event.is_set():
            try:
                # Watchdog: Protect source from lag
                await check_and_protect_source(self.settings)
                # Observability: Log pgai status
                await self._log_pgai_status()
            except RuntimeError as e:
                if "Self-destructed" in str(e):
                    logger.critical(f"Replicator stopping: {e}")
                    self._stop_event.set()
                    break
            except Exception as e:
                logger.error(f"Error in replication watchdog: {e}")

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                continue

    async def start(self):
        """Start all managed services."""
        # 1. Local Postgres
        if self.settings.sink_url == "local":
            self._start_local_postgres()
            if not await self._wait_for_pg(self.settings.local_port):
                raise RuntimeError("Failed to start local Postgres")
            logger.info("Local Postgres is ready.")

        # 2. Initialize connection pools
        await init_pools(self.settings)

        # 3. Setup Source/Sink (Idempotent)
        max_retries = 5
        for i in range(max_retries):
            try:
                await setup_source(self.settings)
                await setup_sink(self.settings)
                break
            except Exception as e:
                if i == max_retries - 1:
                    raise RuntimeError(f"Failed to setup Source/Sink: {e}")
                logger.warning(
                    f"Setup attempt {i+1} failed. Retrying in 5s... {e}"
                )
                await asyncio.sleep(5)

        # 3. Start pgai Worker
        worker = Worker(
            db_url=self.settings.resolved_sink_url,
            poll_interval=timedelta(seconds=2.0),
        )
        self._tasks.append(
            asyncio.create_task(worker.run(), name="pgai_worker")
        )

        # 4. Start Replication Watchdog
        self._tasks.append(
            asyncio.create_task(self._replication_loop(), name="watchdog")
        )

    async def stop(self):
        """Gracefully stop all services."""
        logger.info("Shutting down orchestrator...")
        self._stop_event.set()

        for task in self._tasks:
            task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks = []

        try:
            await drop_subscription_completely(self.settings)
        except Exception as e:
            logger.debug(f"Failed to drop subscription during stop: {e}")

        # Close pools before stopping Postgres
        await close_pools()

        if self._pg_process:
            logger.info("Stopping local Postgres...")
            self._pg_process.terminate()
            try:
                self._pg_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._pg_process.kill()
            self._pg_process = None
            logger.info("Local Postgres stopped.")
