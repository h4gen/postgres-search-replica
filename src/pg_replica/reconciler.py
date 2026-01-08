import logging
from dataclasses import dataclass
from typing import Any, Optional, List, Set, Dict
from enum import Enum
from .config import Settings
from .config import SearchPipeline
from .database import (
    get_source_conn,
    get_sink_conn,
    get_source_column_types,
    setup_source,
    setup_state_table,
    ensure_sink_raw_table,
    setup_sink,
    create_placeholder_slot,
    run_sql_catchup,
    find_and_fix_ghost_records,
    update_replica_state,
    warm_up_from_cache,
    atomic_view_swap,
    drop_subscription_completely,
    cleanup_vectorizer_infrastructure,
    ensure_embedding_cache_table,
    get_vectorizer_statuses,
    ensure_outbox_infrastructure,
    setup_outbox_trigger,
    log_experiment_start,
    log_experiment_finish,
    audit_pipeline_failures,
    ensure_config_history_table,
    get_latest_table_config,
    update_config_status,
    reconciliation_lock,
)

logger = logging.getLogger(__name__)


class ActionType(Enum):
    SOURCE_SETUP = "source_setup"
    SINK_TABLE_EVOLVE = "sink_table_evolve"
    SINK_VECTORIZER_SETUP = "sink_vectorizer_setup"
    SINK_VIEW_SETUP = "sink_view_setup"
    SINK_STATE_INIT = "sink_state_init"
    SINK_RECOVERY = "sink_recovery"
    SINK_CACHE_SETUP = "sink_cache_setup"
    SINK_TABLE_CLEANUP = "sink_table_cleanup"
    SINK_OUTBOX_SETUP = "sink_outbox_setup"


@dataclass
class Action:
    type: ActionType
    description: str
    params: Dict[str, Any]
    target_name: Optional[str] = None  # The key in settings.tables
    is_transactional: bool = True


class Inspector:
    """Discovers current state from Source and Sink databases."""

    def __init__(self, settings: Settings):
        self.settings = settings

    async def get_sink_state(self) -> Dict[str, Any]:
        state: Dict[str, Any] = {
            "tables": {},
            "views": set(),
            "view_targets": {},  # Map: config_name -> current_view_target
            "extensions": set(),
            "replica_states": {}, # Map: config_name -> replica_state
            "vectorizers": {},
            "vectorizer_statuses": {},
        }
        async with await get_sink_conn() as conn:
            async with conn.cursor() as cur:
                # 1. Extensions
                await cur.execute("SELECT extname FROM pg_extension")
                state["extensions"] = {r[0] for r in await cur.fetchall()}

                # 2. Tables & Columns
                await cur.execute(
                    """
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                    """
                )
                table_names = {r[0] for r in await cur.fetchall()}
                for table in table_names:
                    await cur.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                        (table,),
                    )
                    state["tables"][table] = {
                        r[0] for r in await cur.fetchall()
                    }

                # 3. Views
                await cur.execute(
                    "SELECT table_name FROM information_schema.views WHERE table_schema = 'public'"
                )
                state["views"] = {r[0] for r in await cur.fetchall()}

                # 3.5 Triggers
                await cur.execute(
                    "SELECT trigger_name FROM information_schema.triggers"
                )
                state["triggers"] = {r[0] for r in await cur.fetchall()}

                # 4. Table-specific view targets and replica states
                for name, config in self.settings.pipelines.items():
                    # View Target
                    # v7 Implied View Name: {table}_search
                    view_name = f"{config.ingest.table}_search"
                    
                    if view_name in state["views"]:
                        await cur.execute(
                            """
                            SELECT table_name 
                            FROM information_schema.view_table_usage 
                            WHERE view_name = %s
                            """,
                            (view_name,),
                        )
                        rows = await cur.fetchall()
                        target = None
                        version_id = config.get_version_id()
                        for (t,) in rows:
                            if t.endswith(f"_v{version_id}") and "_store_" in t:
                                target = t
                                break
                        if not target:
                             for (t,) in rows:
                                if "_store_v" in t:
                                    target = t
                                    break
                        if not target and rows:
                             target = rows[0][0]
                        state["view_targets"][name] = target

                    # Replica State
                    # Each table has its own state entry keyed by subscription name
                    sub_name = f"sub_{name}"
                    if "_replica_state" in state["tables"]:
                        cols = state["tables"]["_replica_state"]
                        query_cols = ["last_id", "last_lsn"]
                        if "config_hash" in cols:
                            query_cols.append("config_hash")

                        cols_str = ", ".join(query_cols)
                        await cur.execute(
                            f"SELECT {cols_str} FROM _replica_state WHERE key = %s",
                            (sub_name,),
                        )
                        r = await cur.fetchone()
                        if r:
                            state["replica_states"][name] = dict(zip(query_cols, r))

                # 5. Outbox & Mirror Handshake State
                state["outbox_watermarks"] = {}
                if "_sink_outbox" in state["tables"]:
                    await cur.execute(
                        "SELECT version_id, MAX(id) FROM _sink_outbox GROUP BY version_id"
                    )
                    state["outbox_watermarks"] = {r[0]: r[1] for r in await cur.fetchall()}

                state["mirror_progress"] = {}
                if "_sink_mirror_registry" in state["tables"]:
                    await cur.execute(
                        "SELECT mirror_id, target_name, last_processed_id FROM _sink_mirror_registry"
                    )
                    state["mirror_progress"] = {(r[0], r[1]): r[2] for r in await cur.fetchall()}

                # 5. Vectorizers
                if "ai" in state["extensions"]:
                    await cur.execute(
                        """
                        SELECT 
                            source_table::text, 
                            id,
                            config->'destination'->>'target_table' as target_table,
                            config->'destination'->>'view_name' as view_name,
                            name
                        FROM ai.vectorizer
                        """
                    )
                    for src, vid, target, view, v_name in await cur.fetchall():
                        clean_src = src.split(".")[-1]
                        if clean_src not in state["vectorizers"]:
                            state["vectorizers"][clean_src] = []
                        state["vectorizers"][clean_src].append(
                            {
                                "id": vid,
                                "target_table": target,
                                "view_name": view,
                                "name": v_name,
                            }
                        )

                # 6. Vectorizer Sync Status
                state["vectorizer_statuses"] = await get_vectorizer_statuses(self.settings)

                # 7. Control Plane Config State
                state["config_history"] = {}
                if "_replica_config_history" in state["tables"]:
                    for name in self.settings.pipelines.keys():
                        latest = await get_latest_table_config(self.settings, name)
                        if latest:
                            state["config_history"][name] = latest

        return state

    async def get_source_state(self) -> Dict[str, Any]:
        state: Dict[str, Any] = {
            "publications": {},
            "slots": set(),
        }
        async with await get_source_conn() as conn:
            async with conn.cursor() as cur:
                # Get publications and their tables/where clauses
                # Note: This now queries all publications on the source
                await cur.execute(
                    """
                    SELECT pubname, rowfilter, tablename
                    FROM pg_publication_tables 
                    WHERE schemaname = 'public'
                    """
                )
                for pubname, rowfilter, tablename in await cur.fetchall():
                    if pubname not in state["publications"]:
                        state["publications"][pubname] = {"tables": {}}
                    state["publications"][pubname]["tables"][tablename] = {"rowfilter": rowfilter}

                await cur.execute("SELECT slot_name FROM pg_replication_slots")
                state["slots"] = {r[0] for r in await cur.fetchall()}
                logger.debug(f"Discovered source slots: {state['slots']}")
        return state


class Planner:
    """Diffs discovered state against Settings and generates a plan."""

    def __init__(self, settings: Settings):
        self.settings = settings

    def plan(
        self, source_state: Dict[str, Any], sink_state: Dict[str, Any]
    ) -> List[Action]:
        actions = []

        # 1. Global Sink Setup
        if "_replica_state" not in sink_state["tables"]:
            actions.append(
                Action(
                    type=ActionType.SINK_STATE_INIT,
                    description="Initialize replica state table",
                    params={},
                )
            )
        elif "config_hash" not in sink_state["tables"]["_replica_state"]:
            actions.append(
                Action(
                    type=ActionType.SINK_TABLE_EVOLVE,
                    description="Add config_hash column to _replica_state",
                    params={
                        "mode": "alter",
                        "table": "_replica_state",
                        "columns": ["config_hash"],
                    },
                )
            )
        # 1.5 Global Outbox Setup
        if "_sink_outbox" not in sink_state["tables"]:
            actions.append(
                Action(
                    type=ActionType.SINK_OUTBOX_SETUP,
                    description="Initialize Universal Outbox infrastructure",
                    params={},
                )
            )




        # 2. Per-Table Setup (declarative pass)
        cache_setup_added = False
        
        # Pre-calculate all managed vectorizer targets to prevent accidental cleanup
        managed_vectorizers = set()
        for _, cfg in self.settings.pipelines.items():
            vid = cfg.get_version_id()
            managed_vectorizers.add(f"{cfg.ingest.table}_store_v{vid}")

        for name, config in self.settings.pipelines.items():
            if "_embedding_cache" not in sink_state["tables"] and not cache_setup_added:
                actions.append(
                    Action(
                        type=ActionType.SINK_CACHE_SETUP,
                        description="Setup embedding cache table",
                        params={},
                        target_name=name,
                    )
                )
                cache_setup_added = True

            pub_name = f"pub_{name}"
            sub_name = f"sub_{name}"
            desired_hash = config.get_config_hash()
            version_id = config.get_version_id()
            
            replica_state = sink_state["replica_states"].get(name)
            current_hash = replica_state["config_hash"] if replica_state else None

            # 2.1 Source Publication Setup
            needs_source_setup = False
            if pub_name not in source_state["publications"]:
                needs_source_setup = True
            else:
                pub_tables = source_state["publications"][pub_name]["tables"]
                if config.ingest.table not in pub_tables:
                    needs_source_setup = True
                else:
                    current_filter = pub_tables[config.ingest.table]["rowfilter"]
                    desired_filter = (
                        f"({config.ingest.filter})"
                        if config.ingest.filter
                        else None
                    )
                    if current_filter != desired_filter:
                        needs_source_setup = True

            if needs_source_setup:
                if self.settings.source_managed_by_admin:
                    logger.warning(
                        f"Publication {pub_name} drift detected and source is admin-managed."
                    )
                else:
                    actions.append(
                        Action(
                            type=ActionType.SOURCE_SETUP,
                            description=f"Setup/Update publication {pub_name} for {config.ingest.table}",
                            params={},
                            target_name=name,
                        )
                    )

            # 2.2 Sink Table Evolution (Raw Table)
            raw_table = config.ingest.table
            if raw_table not in sink_state["tables"]:
                actions.append(
                    Action(
                        type=ActionType.SINK_TABLE_EVOLVE,
                        description=f"Create sink table {raw_table}",
                        params={"mode": "create", "table": raw_table},
                        target_name=name,
                    )
                )
            else:
                desired_cols = set(config.ingest.columns)
                current_cols = sink_state["tables"][raw_table]
                missing_cols = desired_cols - current_cols
                if missing_cols:
                    actions.append(
                        Action(
                            type=ActionType.SINK_TABLE_EVOLVE,
                            description=f"Add missing columns to {raw_table}: {missing_cols}",
                            params={
                                "mode": "alter",
                                "table": raw_table,
                                "columns": list(missing_cols),
                            },
                            target_name=name,
                        )
                    )

            # 2.4 Vectorizer Setup (State-Based)
            # We ALWAYS ensure the vectorizer for this config exists, even if not active.
            vectorizers = sink_state.get("vectorizers", {}).get(raw_table, [])
            expected_vectorizer_target = f"{raw_table}_store_v{version_id}"
            
            vectorizer_exists = any(
                v.get("target_table") == expected_vectorizer_target for v in vectorizers
            )

            # 2.3 Recovery (Slot check)
            if sub_name not in source_state["slots"]:
                actions.append(
                    Action(
                        type=ActionType.SINK_RECOVERY,
                        description=f"Perform hybrid recovery for {name} (missing slot {sub_name})",
                        params={},
                        target_name=name,
                    )
                )

            # 2.4 Vectorizer Setup (State-Based)
            # We ALWAYS ensure the vectorizer for this config exists, even if not active.
            vectorizers = sink_state.get("vectorizers", {}).get(raw_table, [])
            expected_vectorizer_target = f"{raw_table}_store_v{version_id}"
            
            vectorizer_exists = any(
                v.get("target_table") == expected_vectorizer_target for v in vectorizers
            )

            if not vectorizer_exists:
                logger.info(f"Planning vectorizer setup for {name} (Active={config.active})")
                actions.append(
                    Action(
                        type=ActionType.SINK_VECTORIZER_SETUP,
                        description=f"Create new pgai vectorizer for {name} version {version_id}",
                        params={"table": raw_table, "version_id": version_id},
                        target_name=name,
                    )
                )
            # 2.4.5 Outbox Trigger Setup
            trigger_name = f"trg_outbox_{name}_{version_id}"
            if vectorizer_exists and trigger_name not in sink_state.get("triggers", set()):
                actions.append(
                    Action(
                        type=ActionType.SINK_OUTBOX_SETUP,
                        description=f"Setup outbox trigger for {name} version {version_id}",
                        params={
                            "vectorizer_name": expected_vectorizer_target,
                            "trigger_name": trigger_name,
                            "version_id": version_id
                        },
                        target_name=name,
                    )
                )


            # 2.5 View Setup (Promotion Logic)
            # Only promote if ACTIVE and SYNCED
            current_view_target = sink_state["view_targets"].get(name)
            pending_items = sink_state["vectorizer_statuses"].get(expected_vectorizer_target, 9999)
            is_synced = pending_items == 0
            
            # Mirror Handshake: Ensure external mirrors caught up to outbox watermark
            if is_synced and config.storage.mirrors:
                outbox_watermark = sink_state.get("outbox_watermarks", {}).get(version_id, 0)
                for mirror in config.storage.mirrors:
                    m_id = mirror.id # Pydantic object now
                    m_progress = sink_state.get("mirror_progress", {}).get((m_id, name), 0)
                    if m_progress < outbox_watermark:
                        logger.info(
                            f"Delaying promotion for {name}: Mirror {m_id} is at {m_progress}, "
                            f"waiting for outbox watermark {outbox_watermark}"
                        )
                        is_synced = False
                        break
            
            # Logic: If active, and synced, ensure view points to it.
            # If not synced, we wait (Blue-Green Holding Pattern).
            if config.active:
                should_promote = False
                
                # Case A: View doesn't exist at all -> Promote immediately (or wait? User prefers wait usually, but initial setup needs access)
                if f"{config.ingest.table}_search" not in sink_state["views"]:
                    should_promote = True
                
                # Case B: View exists but points to wrong target (or outdated hash)
                elif (current_view_target != expected_vectorizer_target) or (replica_state and replica_state["config_hash"] != desired_hash):
                    # SAFETY: If the view is pointing to NOTHING (bootstrap or broken view), 
                    # OR if it's the expected target, we promote regardless of sync if missing.
                    if current_view_target is None:
                         should_promote = True
                    elif is_synced:
                        should_promote = True
                    else:
                        logger.info(f"Skipping promotion for {name}: Target {expected_vectorizer_target} has {pending_items} pending items.")
                
                # Case C: Generation check (Ensure we promote if a new generation is Ready but not yet observed)
                # This is handled by the hash check above if the hash changed, 
                # but what if just the generation changed (e.g. forced rebuild)?
                # Actually, config_hash is derived from the JSON, so it should change.
                
                if should_promote:
                    # Idempotency check handled by conditions above
                     actions.append(
                        Action(
                            type=ActionType.SINK_VIEW_SETUP,
                            description=f"Setup/Swap search view {config.ingest.table}_search -> {expected_vectorizer_target} (active=True, synced=True)",
                            params={
                                "config_hash": desired_hash,
                                "target_table": raw_table,
                                "version_id": version_id,
                            },
                            target_name=name,
                        )
                    )

            # 2.6 Cleanup (Global Safety)
            # Explicitly protect the LIVE view target, even if it's not "active" in config
            # (e.g. during a migration where we just flipped active=True to v2, but v2 isn't ready yet, v1 is still live)
            
            real_live_target = sink_state["view_targets"].get(name)
            
            for v in vectorizers:
                v_target = v.get("target_table")
                
                # Policy: Delete if:
                # 1. Not the expected target for THIS config iteration (Optimization: handled by managed set)
                # 2. Not in the "managed_vectorizers" set from ANY config (Global knowledge)
                # 3. Not the currently LIVE target (Safety guard)
                
                is_managed = v_target in managed_vectorizers
                is_live = v_target == real_live_target
                
                if not is_managed and not is_live:
                    actions.append(
                        Action(
                            type=ActionType.SINK_TABLE_CLEANUP,
                            description=f"Cleanup orphaned vectorizer {v.get('id')} ({v_target})",
                            params={
                                "id": v.get("id"),
                                "target_table": v_target,
                            },
                            target_name=name,
                        )
                    )

        return actions


class Applier:
    """Executes the plan generated by the Planner."""

    def __init__(self, settings: Settings):
        self.settings = settings

    async def apply(self, actions: List[Action]) -> Set[str]:
        failed_targets = set()
        
        for action in actions:
            # Skip actions for targets that have already failed in this batch
            if action.target_name and action.target_name in failed_targets:
                continue
                
            logger.info(f"Applying action: {action.description}")
            try:
                target_name = action.target_name
                config = (
                    self.settings.pipelines[target_name]
                    if target_name and target_name in self.settings.pipelines
                    else None
                )

                if action.type == ActionType.SOURCE_SETUP:
                    await setup_source(self.settings, config, target_name)

                elif action.type == ActionType.SINK_STATE_INIT:
                    # This could be triggered for a specific table or globally
                    # If target_name is present, it's a table-specific state entry
                    await setup_state_table(self.settings, target_name or "global")

                elif action.type == ActionType.SINK_TABLE_EVOLVE:
                    table = action.params["table"]
                    if action.params["mode"] == "create":
                        await ensure_sink_raw_table(self.settings, config)
                    else:
                        source_types = (
                            {"config_hash": "TEXT"}
                            if table == "_replica_state"
                            else await get_source_column_types(self.settings, config)
                        )

                        async with await get_sink_conn() as conn:
                            await conn.set_autocommit(True)
                            async with conn.cursor() as cur:
                                for col in action.params["columns"]:
                                    dtype = source_types.get(col, "TEXT")
                                    logger.info(f"Adding column {col} ({dtype}) to {table}")
                                    await cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")

                elif action.type == ActionType.SINK_VECTORIZER_SETUP:
                    # In V7, we use config.ingest.table as sink_raw_table default
                    raw_table = config.ingest.table
                    logger.debug(f"Handling SINK_VECTORIZER_SETUP for {target_name}: config.ingest.table={raw_table}")
                    version_id = action.params.get("version_id", "latest")
                    vectorizer_target = f"{raw_table}_store_v{version_id}"
                    
                    # NOTE: cleanup_vectorizer_infrastructure needs 'config' object. 
                    # If config is SearchPipeline, we must ensure database.py accepts it.
                    # This patch assumes database.py handles it OR we update database.py next.
                    await cleanup_vectorizer_infrastructure(self.settings, config, vectorizer_target)
                    await setup_sink(self.settings, config, target_name, vectorizer_target=vectorizer_target)
                    await warm_up_from_cache(self.settings, config, raw_table, vectorizer_target)
                    await setup_outbox_trigger(self.settings, target_name, vectorizer_target, config)
                    await log_experiment_start(self.settings, target_name, version_id)

                elif action.type == ActionType.SINK_OUTBOX_SETUP:
                    if not target_name:
                        await ensure_outbox_infrastructure(self.settings)
                    else:
                        await setup_outbox_trigger(
                            self.settings,
                            target_name,
                            action.params["vectorizer_name"],
                            config,
                        )

                elif action.type == ActionType.SINK_VIEW_SETUP:
                    await atomic_view_swap(
                        self.settings,
                        config,
                        target_name,
                        action.params["config_hash"],
                        target_table=config.ingest.table,
                        vectorizer_target=f"{config.ingest.table}_store_v{action.params['version_id']}",
                        # We might need to pass 'profile' here? 
                        # database.atomic_view_swap signature needs 'profile' arg to handle hybrid view creation?
                        # It currently takes *args. Let's check signature later.
                        # For now, we update target_table logic.
                    )
                    await log_experiment_finish(self.settings, target_name, action.params["version_id"])

                elif action.type == ActionType.SINK_RECOVERY:
                    lsn = await create_placeholder_slot(self.settings, target_name)
                    await update_replica_state(self.settings, target_name, lsn=lsn)
                    await run_sql_catchup(self.settings, config, target_name)
                    await find_and_fix_ghost_records(self.settings, config, target_name)

                elif action.type == ActionType.SINK_CACHE_SETUP:
                    await ensure_embedding_cache_table(self.settings, config)

                elif action.type == ActionType.SINK_TABLE_CLEANUP:
                    # Cleanup orphaned vectorizer
                    vid = action.params["id"]
                    if vid:
                         # Implement cleanup call if needed, currently placeholder in logic above
                         pass

            except Exception as e:
                logger.error(f"Failed to apply action '{action.description}': {e}", exc_info=True)
                if action.target_name:
                    failed_targets.add(action.target_name)
                    gen = getattr(self.settings.pipelines.get(action.target_name, {}), "_generation", None)
                    if gen:
                        await update_config_status(self.settings, action.target_name, gen, "Failed", error_message=str(e))
                # Continue to next action
        
        return failed_targets



class Reconciler:
    """Orchestrates the Discovery -> Plan -> Apply lifecycle."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.inspector = Inspector(settings)
        self.planner = Planner(settings)
        self.applier = Applier(settings)

    async def reconcile(self):
        logger.info("Starting reconciliation loop...")

        try:
            async with reconciliation_lock():
                # 0. Initialize Infrastructure
                await ensure_config_history_table(self.settings)
                
                # 0.5 Control Plane Override
                # Load latest configs from DB to override in-memory settings
                # We fetch ALL unique target names from the history to allow discovery of new tables
                async with await get_sink_conn() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT DISTINCT target_name FROM _replica_config_history")
                        all_db_targets = [row[0] for row in await cur.fetchall()]

                # Merge DB targets with in-memory targets
                all_targets = set(self.settings.pipelines.keys()) | set(all_db_targets)
                
                for name in all_targets:
                    latest_db = await get_latest_table_config(self.settings, name)
                    if latest_db:
                        # Convert JSON back to SearchPipeline
                        try:
                            # Parse JSON into SearchPipeline (V7)
                            # NOTE: This assumes the DB contains valid V7 JSON. 
                            new_config = SearchPipeline(**latest_db["config_json"])
                            
                            # Attach metadata needed for status updates and checks
                            setattr(new_config, "_generation", latest_db["generation"])
                            setattr(new_config, "_version", latest_db.get("version_id")) # Might be None if not planned
                            
                            self.settings.pipelines[name] = new_config
                            logger.info(f"Loaded dynamic override for {name} (Gen: {latest_db['generation']})")
                        except Exception as e:
                            logger.warning(f"Failed to load dynamic override for {name}: {e}")

                # 1. Discovery
                source_state = await self.inspector.get_source_state()
                sink_state = await self.inspector.get_sink_state()
                
                # 1.5 Audit failures (sync from pgai)
                await audit_pipeline_failures(self.settings)

                # 2. Planning
                actions = self.planner.plan(source_state, sink_state)

                if not actions:
                    logger.info(
                        "No infrastructure drift detected. Everything is in sync."
                    )
                    # Update status of Ready configs if they weren't yet marked observed
                    for name, config in self.settings.pipelines.items():
                        gen = getattr(config, "_generation", None)
                        if gen:
                            await update_config_status(
                                self.settings, name, gen, "Ready", observed_generation=gen
                            )
                    return

                # 3. Application
                # 3. Application
                try:
                    # Mark active configs as Syncing if we have actions for them
                    affected_targets = {a.target_name for a in actions if a.target_name}
                    for target in affected_targets:
                        gen = getattr(self.settings.pipelines.get(target, {}), "_generation", None)
                        if gen:
                            await update_config_status(self.settings, target, gen, "Syncing")

                    failed_targets = await self.applier.apply(actions)
                    
                    # Mark as Ready if successful (excluding failed targets)
                    successful_targets = affected_targets - failed_targets
                    for target in successful_targets:
                        gen = getattr(self.settings.pipelines.get(target, {}), "_generation", None)
                        if gen:
                            await update_config_status(self.settings, target, gen, "Ready", observed_generation=gen)

                except Exception as e:
                    logger.error("Error applying actions", exc_info=True)
                    # Mark as Failed
                    for target in affected_targets:
                        gen = getattr(self.settings.pipelines.get(target, {}), "_generation", None)
                        if gen:
                            await update_config_status(self.settings, target, gen, "Failed", error_message=str(e))
                    raise e
                    
        except RuntimeError as e:
            if "acquire reconciliation lock" in str(e):
                logger.warning("Another reconciler is already running. Skipping this loop.")
                return
            raise e

        logger.info("Reconciliation complete.")
