import logging
from dataclasses import dataclass
from typing import Any, Optional, List, Set, Dict
from enum import Enum
from .config import Settings, TableConfig
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

                # Check if tables exist
                from .database import wait_for_source_table
                for name, config in self.settings.tables.items():
                    if await wait_for_source_table(self.settings, config, timeout=5):
                        state["tables"][name] = True
                    else:
                        state["tables"][name] = False
                # 4. Table-specific view targets and replica states
                for name, config in self.settings.tables.items():
                    # View Target
                    if config.sink_replica_table in state["views"]:
                        await cur.execute(
                            """
                            SELECT table_name 
                            FROM information_schema.view_table_usage 
                            WHERE view_name = %s
                            """,
                            (config.sink_replica_table,),
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
                        row = await cur.fetchone()
                        if row:
                            state["replica_states"][name] = {
                                "last_id": row[0],
                                "last_lsn": row[1],
                                "config_hash": (
                                    row[2] if "config_hash" in query_cols else None
                                ),
                            }

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


        # 2. Per-Table Setup
        cache_setup_added = False
        for name, config in self.settings.tables.items():
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
                if config.source_table not in pub_tables:
                    needs_source_setup = True
                else:
                    current_filter = pub_tables[config.source_table]["rowfilter"]
                    desired_filter = (
                        f"({config.publication_where})"
                        if config.publication_where
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
                            description=f"Setup/Update publication {pub_name} for {config.source_table}",
                            params={},
                            target_name=name,
                        )
                    )

            # 2.2 Sink Table Evolution (Raw Table)
            raw_table = config.sink_raw_table
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
                desired_cols = set(config.publication_columns)
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

            # 2.4 Vectorizer Setup
            needs_new_vectorizer = current_hash != desired_hash
            vectorizers = sink_state.get("vectorizers", {}).get(raw_table, [])
            
            # Use deterministic target names
            expected_vectorizer_target = f"{raw_table}_store_v{version_id}"
            vectorizer_exists = any(
                v.get("target_table") == expected_vectorizer_target for v in vectorizers
            )

            if not vectorizer_exists or needs_new_vectorizer:
                logger.info(f"Planning vectorizer setup for {name}: raw_table={config.sink_raw_table}, source={config.source_table}")
                actions.append(
                    Action(
                        type=ActionType.SINK_VECTORIZER_SETUP,
                        description=f"Create new pgai vectorizer for {name} version {version_id}",
                        params={"table": raw_table, "version_id": version_id},
                        target_name=name,
                    )
                )

            # 2.5 View Setup
            current_view_target = sink_state["view_targets"].get(name)
            if (
                config.sink_replica_table not in sink_state["views"]
                or current_hash != desired_hash
                or current_view_target != expected_vectorizer_target
            ):
                actions.append(
                    Action(
                        type=ActionType.SINK_VIEW_SETUP,
                        description=f"Setup search view {config.sink_replica_table} (Profile: {config.search_profile})",
                        params={
                            "config_hash": desired_hash,
                            "target_table": raw_table,
                            "version_id": version_id,
                        },
                        target_name=name,
                    )
                )

            # 2.6 Cleanup old vectorizers
            current_live_view_target = sink_state["view_targets"].get(name)
            for v in vectorizers:
                v_target = v.get("target_table")
                if (
                    v_target != expected_vectorizer_target
                    and v_target != current_live_view_target
                ):
                    # SAFETY: Only cleanup if it matches our naming pattern for this config
                    # to avoid deleting vectorizers from other configs sharing the same raw table
                    if v_target.startswith(f"{raw_table}_store_v"):
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

    async def apply(self, actions: List[Action]):
        for action in actions:
            logger.info(f"Applying action: {action.description}")
            target_name = action.target_name
            config = (
                self.settings.tables[target_name]
                if target_name and target_name in self.settings.tables
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
                raw_table = config.sink_raw_table
                logger.debug(f"Handling SINK_VECTORIZER_SETUP for {target_name}: config.sink_raw_table={raw_table}")
                version_id = action.params.get("version_id", "latest")
                vectorizer_target = f"{raw_table}_store_v{version_id}"

                await cleanup_vectorizer_infrastructure(self.settings, config, vectorizer_target)
                await setup_sink(self.settings, config, target_name, vectorizer_target=vectorizer_target)
                await warm_up_from_cache(self.settings, config, raw_table, vectorizer_target)

            elif action.type == ActionType.SINK_VIEW_SETUP:
                await atomic_view_swap(
                    self.settings,
                    config,
                    target_name,
                    action.params["config_hash"],
                    target_table=config.sink_raw_table,
                    vectorizer_target=f"{config.sink_raw_table}_store_v{action.params['version_id']}",
                )

            elif action.type == ActionType.SINK_RECOVERY:
                lsn = await create_placeholder_slot(self.settings, target_name)
                await update_replica_state(self.settings, target_name, lsn=lsn)
                await run_sql_catchup(self.settings, config, target_name)
                await find_and_fix_ghost_records(self.settings, config, target_name)

            elif action.type == ActionType.SINK_CACHE_SETUP:
                await ensure_embedding_cache_table(self.settings, config)

            elif action.type == ActionType.SINK_TABLE_CLEANUP:
                await drop_subscription_completely(self.settings, config, target_name)


class Reconciler:
    """Orchestrates the Discovery -> Plan -> Apply lifecycle."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.inspector = Inspector(settings)
        self.planner = Planner(settings)
        self.applier = Applier(settings)

    async def reconcile(self):
        logger.info("Starting reconciliation loop...")

        # 1. Discovery
        source_state = await self.inspector.get_source_state()
        sink_state = await self.inspector.get_sink_state()

        # 2. Planning
        actions = self.planner.plan(source_state, sink_state)

        if not actions:
            logger.info(
                "No infrastructure drift detected. Everything is in sync."
            )
            return

        # 3. Application
        try:
            await self.applier.apply(actions)
        except Exception as e:
            logger.error("Error applying actions", exc_info=True)
            raise e
        logger.info("Reconciliation complete.")
