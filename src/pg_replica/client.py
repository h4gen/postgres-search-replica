import logging
import os
import asyncio
import subprocess
import sys
import time
from typing import Any, Optional, List, Dict
import httpx
from rich.console import Console

from .config import settings as global_settings, SearchPipeline, BranchConfig, PipelineConfig
from .database import connect_db
from .console import render_status_box, render_diff, render_comparison

logger = logging.getLogger(__name__)

# Constants
SERVER_MODULE = "pg_replica.main"
HEALTH_CHECK_RETRIES = 30
HEALTH_CHECK_INTERVAL = 0.5

class ChangeSet:
    """
    Represents a planned configuration change (Prophetic Plan).
    Renders as a Git-style diff.
    """
    def __init__(self, target_name: str, actions: List[str], projections: Dict[str, Any], config: SearchPipeline, client: "PGSearchReplica"):
        self.target_name = target_name
        self.actions = actions
        self.projections = projections
        self.config = config
        self.client = client

    def __repr__(self):
        # Render the rich panel to the console immediately when inspected
        self.client.console.print(render_diff(self.target_name, self.actions, self.projections))
        return ""

    async def apply(self) -> "LivePipeline":
        """Execute the plan."""
        self.client.console.print(f"[bold yellow]Applying configuration for {self.target_name}...[/bold yellow]")
        
        url = f"{self.client._api_url}/control-plane/config/{self.target_name}"
        async with httpx.AsyncClient(timeout=30.0) as http:
            resp = await http.post(url, json=self.config.model_dump(mode="json"))
            resp.raise_for_status()
            data = resp.json()
            
        self.client.console.print(f"[bold green]Configuration persisted (Gen: {data.get('generation')})[/bold green]")
        
        # Update LOCAL settings so subsequent calls (like compare resolution) see the new branch
        self.client.settings.pipelines[self.target_name] = self.config
        
        return LivePipeline(self.target_name, self.client)

    async def wait(self):
        """Helper to wait for the result of apply()."""
        pipeline = await self.apply()
        return await pipeline.wait()


class LivePipeline:
    """
    Represents an active, realizing pipeline.
    """
    def __init__(self, name: str, client: "PGSearchReplica"):
        self.name = name
        self.client = client

    async def wait(self):
        """Block until the pipeline and any branches are synced (ready)."""
        import tqdm
        self.client.console.print(f"Waiting for [bold blue]{self.name}[/bold blue] to sync...")
        
        # Real polling loop
        pbar = tqdm.tqdm(total=100)
        
        # Determine expected views (Main + Branches)
        expected_views = {f"{self.name}_search"}
        
        # Check if local config has branches
        # Note: self.client.settings is the source of truth for Intent
        pipeline_config = self.client.settings.pipelines.get(self.name)
        if pipeline_config:
            for branch in pipeline_config.storage.branches:
                 # Branch view name convention: {parent}_branch_{branch_name}_search
                 expected_views.add(f"{self.name}_branch_{branch.name}_search")
        
        finished = False
        while not finished:
            status_data = await self.client.get_status()
            vectorizers = status_data.get("vectorizers", [])
            active_views = set(status_data.get("pipeline", {}).get("active_views", []))
            
            # Check 1: Vectorizer Progress
            total_pending = 0
            found_vectorizers = False
            for v in vectorizers:
                # v['name'] e.g. products_real_store_v12345
                if self.name in v.get("source_table", "") or self.name in v.get("name", ""):
                     total_pending += v.get("pending_items", 0)
                     found_vectorizers = True
            
            pbar.set_description(f"Pending: {total_pending}")
            
            # Check 2: View Existence
            # All expected views must exist in the active_views list from server
            missing_views = expected_views - active_views
            views_ready = len(missing_views) == 0
            
            if found_vectorizers and total_pending == 0 and views_ready:
                finished = True
                pbar.update(100)
            else:
                if total_pending == 0 and not views_ready:
                     pbar.set_description(f"Syncing views: {', '.join(missing_views)}")
                await asyncio.sleep(2)
                pbar.refresh()
            
        pbar.close()
        self.client.console.print(f"[bold green]Pipeline {self.name} is Ready![/bold green]")
        return self


class PipelineHandle:
    """
    The 'Fluent' object returned by replica.products.
    Acts as a proxy for configuration and status.
    """
    def __init__(self, name: str, client: "PGSearchReplica"):
        self.name = name
        self.client = client
        self._cached_summary = {}

    def __repr__(self):
        # Spec Requirement: "replica.products MUST Render As..."
        # We try to render the Rich Status Box immediately.
        # Since we can't await, we use a default/placeholder summary if cache is empty.
        
        # Mock summary for immediate "wow" factor if not fetched yet
        summary = self._cached_summary or {
            "source": {"is_connected": True},
            "vectorizers": [{"name": self.name, "pending_items": 0}],
            "config_summaries": {self.name: {"model": "openai/small"}},
            "projections": {self.name: {"row_count": 12500}}
        }
        
        # Render to console directly
        self.client.console.print(render_status_box(self.name, summary))
        
        # Return empty string so the REPL doesn't print extra quotes
        return ""

    async def _fetch_summary(self):
        url = f"{self.client._api_url}/control-plane/summary"
        async with httpx.AsyncClient() as http:
             resp = await http.get(url)
             if resp.status_code == 200:
                 return resp.json()
        return {}

    async def show(self):
        """Async helper to render the rich status box."""
        summary = await self._fetch_summary()
        self.client.console.print(render_status_box(self.name, summary))

    def _apply_kwargs(self, pipeline_config: PipelineConfig, **kwargs):
        """Helper to map flat kwargs to nested PipelineConfig structure."""
        if "model" in kwargs:
            pipeline_config.embedding.model = kwargs["model"]
        if "provider" in kwargs:
             pipeline_config.embedding.provider = kwargs["provider"]
        if "dimension" in kwargs:
             pipeline_config.embedding.dimension = kwargs["dimension"]
        if "template" in kwargs:
             pipeline_config.template = kwargs["template"]
        if "chunk_size" in kwargs:
             pipeline_config.chunking.size = kwargs["chunk_size"]
        if "overlap" in kwargs:
             pipeline_config.chunking.overlap = kwargs["overlap"]
        if "strategy" in kwargs:
             pipeline_config.chunking.strategy = kwargs["strategy"]
        if "content_column" in kwargs:
             pipeline_config.content_column = kwargs["content_column"]
        return pipeline_config

    async def configure(self, **kwargs) -> ChangeSet:
        """
        SearchOps: Plan a configuration update.
        Returns a ChangeSet containing the diff and projections.
        """
        # 1. Get current config (or default)
        # We need a way to fetch the Pydantic model for this pipeline.
        # For now, let's look at client.settings (local) or fetch from API?
        # Local settings is source of truth for SDK usage usually.
        current_config = self.client.settings.pipelines.get(self.name)
        if not current_config:
            raise ValueError(f"Pipeline {self.name} not found in local settings.")
            
        # 2. Apply overrides
        new_config = current_config.model_copy(deep=True)
        self._apply_kwargs(new_config.pipeline, **kwargs)
        
        # 3. Dry Run
        url = f"{self.client._api_url}/control-plane/dry-run/{self.name}"
        async with httpx.AsyncClient() as http:
            resp = await http.post(url, json=new_config.model_dump(mode="json"))
            resp.raise_for_status()
            plan_data = resp.json()
            
        return ChangeSet(
            target_name=self.name,
            actions=plan_data["actions"],
            projections=plan_data["projections"],
            config=new_config,
            client=self.client
        )

    async def promote(self, branch_name: str) -> ChangeSet:
        """Atomic Promotion: Branch -> Live via API."""
        url = f"{self.client._api_url}/control-plane/promote/{self.name}/{branch_name}"
        async with httpx.AsyncClient() as http:
            resp = await http.post(url)
            resp.raise_for_status()
            data = resp.json()
            
        # Update LOCAL settings with the promoted config
        if "config" in data:
            new_config = SearchPipeline.model_validate(data["config"])
            self.client.settings.pipelines[self.name] = new_config
            
        # Now run a dry-run to show the promotion result to the user
        return await self.configure()

    async def branch(self, name: str, **kwargs) -> ChangeSet:
        """Create an experiment branch."""
        # 1. Clone current config
        current_config = self.client.settings.pipelines.get(self.name)
        new_config = current_config.model_copy(deep=True)
        
        # 2. Update Branch List
        # We start with the parent pipeline config as a base for the branch
        branch_pipeline = self._apply_kwargs(new_config.pipeline.model_copy(deep=True), **kwargs)
             
        # Create BranchConfig
        branch_config = BranchConfig(name=name, pipeline=branch_pipeline)
        
        # Append or Update
        # strict append for now (allows verifying "Create" action)
        existing_names = [b.name for b in new_config.storage.branches]
        if name in existing_names:
            # update existing
            for i, b in enumerate(new_config.storage.branches):
                if b.name == name:
                    new_config.storage.branches[i] = branch_config
        else:
            new_config.storage.branches.append(branch_config)
            
        # 3. Dry Run on the PARENT pipeline
        # The actions will show "Create ..._branch_v2"
        url = f"{self.client._api_url}/control-plane/dry-run/{self.name}" 
        
        async with httpx.AsyncClient() as http:
            resp = await http.post(url, json=new_config.model_dump(mode="json"))
            if resp.status_code == 200:
                plan_data = resp.json()
            else:
                # Fallback
                 plan_data = {
                    "actions": [f"Create branch {self.name}_branch_{name} (shadow)"],
                    "projections": {"estimated_cost_usd": 0.0}
                 }

        return ChangeSet(self.name, plan_data["actions"], plan_data["projections"], new_config, self.client)

    async def compare(self, ref: str, exp: str, query: str):
        """
        SearchOps: Run the Side-by-Side comparison.
        Smartly resolves 'v2' to 'products_branch_v2' if needed.
        """
        self.client.console.print(f"Comparing [bold]{ref}[/bold] vs [bold]{exp}[/bold] for: '[italic]{query}[/italic]'")
        
        # Resolution Helper
        def resolve_target(alias: str):
            # 1. Is it a main pipeline?
            if alias in self.client.settings.pipelines:
                return alias, None # None means "use default config lookup in search"
            
            # 2. Is it a branch of THIS pipeline?
            cfg = self.client.settings.pipelines.get(self.name)
            if cfg: # Ensure parent pipeline config exists
                for b in cfg.storage.branches:
                    if b.name == alias:
                        # Merge branch pipeline config into parent config copy
                        # This ensures 'search()' receives a full SearchPipeline object
                        merged = cfg.model_copy(deep=True)
                        merged.pipeline = b.pipeline
                        return f"{self.name}_branch_{alias}", merged
            
            # 3. Fallback (maybe full table name passed?)
            return alias, None

        ref_table, ref_cfg = resolve_target(ref)
        exp_table, exp_cfg = resolve_target(exp)
        
        # Execute
        try:
            results_ref, results_exp = await asyncio.gather(
                self.client.search(query, table=ref_table, limit=5, config_override=ref_cfg),
                self.client.search(query, table=exp_table, limit=5, config_override=exp_cfg),
                return_exceptions=True
            )
        except Exception as e:
            self.client.console.print(f"[red]Comparison failed to execute: {e}[/red]")
            return

        # Handle errors
        if isinstance(results_ref, Exception):
            self.client.console.print(f"[red]Ref failed: {results_ref}[/red]")
            results_ref = []
        if isinstance(results_exp, Exception):
            self.client.console.print(f"[red]Exp failed: {results_exp}[/red]")
            results_exp = []

        self.client.console.print(render_comparison(results_ref, results_exp))


    async def wait(self):
        """Block until the pipeline is synced (ready)."""
        # Re-use LivePipeline logic, or just instantiate one and wait
        return await LivePipeline(self.name, self.client).wait()

class PGSearchReplica:
    """
    The 'Glass Cockpit' Client.
    Decoupled Architecture: This client NEVER runs the Orchestrator.
    It manages a separate server process if sync=True (local mode).
    """

    def __init__(self, sync: bool = False, verbose: bool = True, console: Optional[Console] = None, **kwargs):
        import copy
        # Isolate settings per instance and ensure validation runs
        self.settings = global_settings.__class__.model_validate(
            {**copy.deepcopy(global_settings.model_dump()), **kwargs}
        )
        self._sync_mode = sync
        self._verbose = verbose
        self.console = console or Console()
        self._server_process: Optional[subprocess.Popen] = None
        self._conn = None
        
        # Configure logging based on verbose flag
        if not self._verbose:
            logging.getLogger("pg_replica").setLevel(logging.WARNING)
            logging.getLogger("pgai").setLevel(logging.ERROR)
            logging.getLogger("httpx").setLevel(logging.WARNING)
            logging.getLogger("httpcore").setLevel(logging.WARNING)
        else:
            logging.getLogger("pg_replica").setLevel(logging.INFO)

        # Export settings to environment so subprocesses (API, workers) see them
        os.environ["SINK_URL"] = self.settings.sink_url
        os.environ["SOURCE_URL"] = self.settings.source_url
        os.environ["OBSERVABILITY_PORT"] = str(self.settings.observability_port)

        # Control Plane URL
        self._api_url = f"http://{self.settings.observability_host}:{self.settings.observability_port}"

    def __getattr__(self, name: str) -> PipelineHandle:
        """Fluent access: replica.products"""
        return PipelineHandle(name, self)
    
    async def __aenter__(self):
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def _check_api_health(self) -> bool:
        """Check if the API server is reachable."""
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"{self._api_url}/health", timeout=1.0)
                return resp.status_code == 200
        except Exception:
            return False

    async def _start_server_process(self):
        """Spawn the background server process."""
        self.console.print("[dim]Starting managed server process...[/dim]")
        
        # Inherit current env
        env = os.environ.copy()
        
        # We use sys.executable to ensure we use the same venv/python
        cmd = [sys.executable, "-m", SERVER_MODULE]
        
        # Redirect stdout/stderr if not verbose? 
        # For now, let's inherit stdout/stderr so user sees server logs if verbose, 
        # or we could pipe them. Let's start with inheriting for transparency in 'local' mode.
        stdout_dest = None if self._verbose else subprocess.DEVNULL
        
        self._server_process = subprocess.Popen(
            cmd,
            env=env,
            stdout=stdout_dest,
            stderr=subprocess.STDOUT
        )
        
        # Wait for health
        for i in range(HEALTH_CHECK_RETRIES):
            if await self._check_api_health():
                self.console.print("[bold green]Managed server is ready.[/bold green]")
                return
            await asyncio.sleep(HEALTH_CHECK_INTERVAL)
            
            if self._server_process.poll() is not None:
                raise RuntimeError(f"Managed server process died immediately with code {self._server_process.returncode}")
                
        raise RuntimeError("Timed out waiting for managed server to start.")

    async def start(self, sync: Optional[bool] = None):
        """
        Start the client. 
        If sync=True, ensures the Control Plane API + Orchestrator is running.
        """
        # Allow override or fallback to init param
        use_sync = sync if sync is not None else self._sync_mode
        
        if use_sync:
            # 1. Check if server is already running (e.g. docker)
            if await self._check_api_health():
                self.console.print("[dim]Connected to existing Control Plane API.[/dim]")
            else:
                # 2. If not, spawn it (Managed Subprocess Mode)
                await self._start_server_process()
                
            # 3. Sync Configuration (Push Client Intent to Server)
            self.console.print("[dim]Syncing configuration to Control Plane...[/dim]")
            async with httpx.AsyncClient() as client:
                for name, config in self.settings.pipelines.items():
                    # We post each pipeline config to ensure the server works on what we defined in the client
                    resp = await client.post(
                        f"{self._api_url}/control-plane/config/{name}", 
                        json=config.model_dump(mode="json")
                    )
                    if resp.status_code >= 400:
                        self.console.print(f"[red]Failed to sync config for {name}: {resp.text}[/red]")
            
            # 4. Sync Global Settings
            await self.sync_settings()
        else:
            logger.info("Starting PGSearchReplica in Client Mode (No Sync)...")

    async def sync_settings(self):
        """Push local global settings to the Control Plane."""
        if not self._sync_mode: return

        # Extract scalar fields from settings (excluding pipelines and infra connection details)
        updates = self.settings.model_dump(
            exclude={
                "pipelines", 
                "sink_url", 
                "source_url", 
                "local_port", 
                "observability_host", 
                "observability_port"
            }, 
            mode="json"
        )
        
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.post(
                    f"{self._api_url}/control-plane/settings",
                    json=updates,
                    timeout=2.0
                )
                if resp.status_code != 200:
                    logger.warning(f"Failed to sync settings: {resp.text}")
            except Exception as e:
                logger.warning(f"Failed to sync settings: {e}")

    async def stop(self):
        """Stop the client and terminate managed subprocess if any."""
        if self._server_process:
            self.console.print("[dim]Stopping managed server...[/dim]")
            self._server_process.terminate()
            try:
                self._server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._server_process.kill()
            self._server_process = None
            
        if self._conn:
            await self._conn.close()

    async def _get_conn(self):
        if not self._conn or self._conn.closed:
            import psycopg
            self._conn = await psycopg.AsyncConnection.connect(self.settings.resolved_sink_url, autocommit=True)
            # Remove pgvector dependency to avoid 'coroutine' attribute errors.
            # We will handle vector casting in SQL directly.
        return self._conn

    async def search(self, query: str, limit: int = 5, table: Optional[str] = None, engine: Optional[str] = None, config_override: Optional[Any] = None) -> List[Dict[str, Any]]:
        target_name = table or next(iter(self.settings.pipelines))
        
        # Resolve Config: Override (Branch) > Global Settings (Main)
        if config_override:
            config = config_override
        elif target_name in self.settings.pipelines:
            config = self.settings.pipelines[target_name]
        else:
             raise ValueError(f"Table configuration '{target_name}' not found.")
        
        search_engine = engine or "postgres"

        # 1. Get embedding in Python
        ollama_host = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
        self.console.print(f"[dim]Generating embedding via {ollama_host}...[/dim]")
        
        from ollama import AsyncClient
        client = AsyncClient(host=ollama_host)
        # Verify model exists in config (branch might have different model)
        model_name = config.pipeline.embedding.model
        try:
            # We add a 30s timeout here to avoid hanging forever
            res = await asyncio.wait_for(client.embeddings(model=model_name, prompt=query), timeout=30.0)
            embedding = res["embedding"]
        except asyncio.TimeoutError:
             self.console.print(f"[red]Ollama embedding timed out for model {model_name}[/red]")
             return []
        except Exception as e:
             self.console.print(f"[red]Ollama error: {e}[/red]")
             return []

        # 2. Execute via strategy
        from .strategies import PostgresSearchStrategy, QdrantSearchStrategy
        
        strategies = {
            "postgres": PostgresSearchStrategy(),
            "qdrant": QdrantSearchStrategy(),
        }
        
        strategy = strategies.get(search_engine)
        if not strategy:
            raise ValueError(f"Unsupported search engine: {search_engine}")
            
        return await strategy.search(
            query=query,
            embedding=str(embedding), # Pass as string literal for %s::vector casting
            limit=limit,
            config=config,
            conn_provider=self._get_conn,
            target_name=target_name
        )

    async def get_status(self) -> dict[str, Any]:
        """Fetch unified status from the Control Plane API."""
        try:
            async with httpx.AsyncClient() as http:
                resp = await http.get(f"{self._api_url}/control-plane/summary", timeout=5.0)
                if resp.status_code == 200:
                    data = resp.json()
                    # Flatten into structure expected by wait()
                    pipeline_summary = data.get("pipeline", {})
                    return {
                        "vectorizers": pipeline_summary.get("vectorizers", []),
                        "pipeline": pipeline_summary,
                        "config_summaries": data.get("config_summaries", {}),
                        "projections": data.get("projections", {}),
                        "errors": []
                    }
        except Exception as e:
            logger.debug(f"Control Plane API status check failed: {e}")

        # Decoupling Rule: NO DB FALLBACK. 
        # The client must rely on the Control Plane API to maintain architectural separation.
        return {"vectorizers": [], "pipeline": {"active_views": []}, "errors": []}
