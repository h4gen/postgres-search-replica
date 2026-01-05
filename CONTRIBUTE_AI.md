# AI Contribution Guide: CI Reliability & Local Development

This project uses a strict **Environment-First** testing philosophy. Failures in the cloud (GitHub Actions) are usually caused by ignoring the synchronization between the infrastructure state and the test runner.

## 1. The CI Reproduction Protocol
The Cloud CI (see `.github/workflows/ci.yml`) follows a precise sequence. To guarantee your changes will pass in the cloud, you **must** be able to run this local shorthand successfully:

```bash
make test-dev
```

This command executes the following critical lifecycle:
1. `make clean`: Destroys all previous volumes and stale replication slots. **Never skip this.**
2. `make dev`: Rebuilds containers from the latest source edits.
3. `make wait-for-infra`: **The Stability Gate.** It blocks until Postgres extensions are loaded, Ollama models are pulled, and Qdrant health checks pass.
4. `make test`: Runs the distributed suite.
5. `make down`: Graceful cleanup.

## 2. The "Log Audit" Protocol (Critical)
**Green tests do not guarantee success.** In a system with asynchronous background workers (`Reconciler`, `MirrorWorker`), logic errors often manifest as "Swallowed Exceptions" that log an error but don't exit the process.

### Mandatory Verification Steps:
1. **Run Scoped Tests Verbously**:
   ```bash
   make test-integration ARGS="tests/test_filename.py -s"
   ```
2. **Audit for "ERROR" and "WARNING"**: Before submitting, you must manually grep/scan the test output for:
   - `psycopg.errors`: Often indicates triggers firing on missing tables.
   - `Failed to sync batch`: Indicates the data plane is broken while the test plane is idling.
   - `teardown source error`: Stale replication slots that will block the *next* test run.

## 3. Maintaining CI Parity
- **Environment Variables**: The `Makefile` sets `SOURCE_URL` and `SINK_URL` to match the Docker Compose setup. If you change a port in `docker-compose.yml`, you **must** update the `Makefile` defaults or CI will hang.
- **Dependency Sync**: If you add a library, run `uv add` and ensure it's in the `test` extra. The CI runs `uv sync --extra test` every time.
- **Wait Logic**: If you add a new service (e.g., a new cache or search engine), you **must** add a health check to the `wait-for-infra` target in the `Makefile`. CI does not have a human to wait; if the service isn't ready when `pytest` starts, the run is a failure.

## 4. Troubleshooting Cloud Failures
If it passes locally but fails in CI:
1. **Model Latency**: CI may take longer to pull the Ollama embedding model. Check if `make wait-for-infra` timed out.
2. **Race Conditions**: Use `asyncio.sleep` sparingly in tests; instead, use the "Wait-Until-State" patterns found in `tests/test_search_strategies.py`.
3. **Ghost State**: If the previous CI run crashed, it might have left a volume or a slot. `make clean` is your only savior.

---

**Final Rule**: If you didn't see the Reconciler logs moving in your terminal, the test didn't happen.
