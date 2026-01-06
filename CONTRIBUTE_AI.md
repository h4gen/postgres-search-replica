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
3. **The Mandatory Grep Protocol**:
   Run the following command after `make test-dev` and ensure zero unexpected hits:
   ```bash
   grep -E "ERROR|WARNING" test_output.log | grep -v "Expected Failure Case"
   ```
   *Note: Real architectural errors are often swallowed as logs in background workers. If you see a log that shouldn't be there, the test is a failure.*

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

## 5. UI Contribution & The Benchmarking Protocol

The **Search Engineer Workbench** is a decoupled client application. To maintain architectural purity, follow these strict rules:

### The "Decoupling" Mandate
- **UI is an Observer**: The UI only consumes data via the `/control-plane/*` endpoints in `observability.py`. 
- **Zero Business Logic Touch**: Contributing to the UI **must not** involve modifying `database.py`, `reconciler.py`, or `orchestrator.py`. If you need more data, enrich the Observability API, not the core persistence layer.
- **Industrial Aesthetic**: Use base Shadcn UI components. Avoid "flashy" styles like glassmorphism. Focus on information density, monospace fonts for data, and industrial utility.

### The UI Development Flow

Before running the heavy, infrastructure-dependent UI suite, perform rapid diagnostics:

1.  **Scope Check**: Run `make check` to catch any Python import or syntax errors instantly.
2.  **API Unit Tests**: Run `make test-obs` to verify the Observability API endpoints (`/health`, `/summary`, `/dry-run`) in isolation without Docker.
3.  **UI Build**: Run `make ui-build` to ensure Next.js type-safety and build stability.

Only after these pass should you proceed to develop or debug the UI using the local benchmarking target:
```bash
make test-ui
```
This command:
1.  **Seeds Reality**: Runs `tests/ui_benchmark_data.py` to register persisted table configs in the Sink DB.
2.  **Simulates Traffic**: Starts `tests/live_data_generator.py` in the background to perform continuous CRUD on the source. This drives live sparklines and lag metrics.
3.  **Orchestrates**: Starts the Next.js dev server on port `3001`.

### Verification via Browser Actions (For AI Agents)
When verifying UI changes, you **must** use a browser subagent to:
1.  **Confirm Reactivity**: Wait 10s and verify that "Sync Latency" or "LSN Position" values are updating. If they are static, the connection to the Observability API or the live generator is broken.
2.  **Verify Progress Bars**: Ensure `pgai` statuses are visible and reporting percentage progress.
3.  **Test the Lab**: Navigate to `/settings`, select a table, and perform a "Pre-flight Check" (Dry Run). Confirm the projection cards appear with RAM and Action estimates.
4.  **Audit Aesthetic**: Ensure the UI looks like an industrial control plane (sharp edges, terminal-style logs, professional density).
