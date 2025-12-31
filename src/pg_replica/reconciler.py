import logging
from dataclasses import dataclass
from typing import Any, Optional, List, Set, Dict
from enum import Enum
from .config import Settings
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
    is_transactional: bool = True


class Inspector:
    """Discovers current state from Source and Sink databases."""

    def __init__(self, settings: Settings):
        self.settings = settings

    async def get_sink_state(self) -> Dict[str, Any]:
        state = {
            "tables": {},
            "views": set(),
            "view_target": None,
            "extensions": set(),
            "replica_state": None,
            "vectorizers": {},
        }
        async with await get_sink_conn() as conn:
            async with conn.cursor() as cur:
                # 1. Extensions
                await cur.execute("SELECT extname FROM pg_extension")
                state["extensions"] = {r[0] for r in await cur.fetchall()}

                # 2. Tables
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

                # 3. Views and their underlying tables
                await cur.execute(
                    "SELECT table_name FROM information_schema.views WHERE table_schema = 'public'"
                )
                state["views"] = {r[0] for r in await cur.fetchall()}

                # Get view usage to see which table the replica view points to
                await cur.execute(
                    """
                    SELECT table_name 
                    FROM information_schema.view_table_usage 
                    WHERE view_name = %s
                    """,
                    (self.settings.sink_replica_table,),
                )
                row = await cur.fetchone()
                state["view_target"] = row[0] if row else None

                # 4. Replica State (including our new config_hash)
                if "_replica_state" in state["tables"]:
                    cols = state["tables"]["_replica_state"]
                    query_cols = ["last_id", "last_lsn"]
                    if "config_hash" in cols:
                        query_cols.append("config_hash")

                    cols_str = ", ".join(query_cols)
                    await cur.execute(
                        f"SELECT {cols_str} FROM _replica_state WHERE key = %s",
                        (self.settings.subscription_name,),
                    )
                    row = await cur.fetchone()
                    if row:
                        state["replica_state"] = {
                            "last_id": row[0],
                            "last_lsn": row[1],
                            "config_hash": (
                                row[2] if "config_hash" in query_cols else None
                            ),
                        }

                # 5. Vectorizers (if pgai is installed)
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
                    state["vectorizers"] = {}
                    for src, vid, target, view, name in await cur.fetchall():
                        # source_table can be 'public.products' or just 'products'
                        clean_src = src.split(".")[-1]
                        if clean_src not in state["vectorizers"]:
                            state["vectorizers"][clean_src] = []
                        state["vectorizers"][clean_src].append(
                            {
                                "id": vid,
                                "target_table": target,
                                "view_name": view,
                                "name": name,
                            }
                        )

        return state

    async def get_source_state(self) -> Dict[str, Any]:
        state = {
            "publications": {},
            "slots": set(),
        }
        async with await get_source_conn() as conn:
            async with conn.cursor() as cur:
                # Get publications and their tables/where clauses
                await cur.execute(
                    """
                    SELECT pubname, rowfilter 
                    FROM pg_publication_tables 
                    WHERE schemaname = 'public' AND tablename = %s
                    """,
                    (self.settings.source_table,),
                )
                for pubname, rowfilter in await cur.fetchall():
                    state["publications"][pubname] = {"rowfilter": rowfilter}

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
        desired_hash = self.settings.get_config_hash()
        current_hash = (
            sink_state["replica_state"]["config_hash"]
            if sink_state["replica_state"]
            else None
        )

        # 1. Source Setup
        needs_source_setup = False
        if self.settings.publication_name not in source_state["publications"]:
            needs_source_setup = True
        else:
            current_filter = source_state["publications"][
                self.settings.publication_name
            ]["rowfilter"]
            desired_filter = (
                f"({self.settings.publication_where})"
                if self.settings.publication_where
                else None
            )
            if current_filter != desired_filter:
                needs_source_setup = True

        if needs_source_setup:
            if self.settings.source_managed_by_admin:
                logger.warning(
                    f"Publication {self.settings.publication_name} drift detected and source is admin-managed."
                )
            else:
                actions.append(
                    Action(
                        type=ActionType.SOURCE_SETUP,
                        description=f"Setup/Update publication {self.settings.publication_name}",
                        params={},
                    )
                )

        # 2. Sink State Table & Init
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

        # 3. Cache Setup
        if "_embedding_cache" not in sink_state["tables"]:
            actions.append(
                Action(
                    type=ActionType.SINK_CACHE_SETUP,
                    description="Setup embedding cache table",
                    params={},
                )
            )

        # 4. Table Evolution
        # For Blue-Green, we keep the raw table stable but create unique vectorizers
        target_table = self.settings.sink_raw_table

        if target_table not in sink_state["tables"]:
            actions.append(
                Action(
                    type=ActionType.SINK_TABLE_EVOLVE,
                    description=f"Create sink table {target_table}",
                    params={"mode": "create", "table": target_table},
                )
            )
        else:
            desired_cols = set(self.settings.publication_columns)
            current_cols = sink_state["tables"][target_table]
            missing_cols = desired_cols - current_cols
            if missing_cols:
                actions.append(
                    Action(
                        type=ActionType.SINK_TABLE_EVOLVE,
                        description=f"Add missing columns to {target_table}: {missing_cols}",
                        params={
                            "mode": "alter",
                            "table": target_table,
                            "columns": list(missing_cols),
                        },
                    )
                )

        # 5. Vectorizer Setup
        version_id = self.settings.get_version_id()
        needs_new_vectorizer = current_hash != desired_hash
        vectorizers = sink_state.get("vectorizers", {}).get(target_table, [])

        # Check if a vectorizer with the same name exists
        current_vectorizer_target = f"{target_table}_store_v{version_id}"
        current_vectorizer_name = current_vectorizer_target
        vectorizer_exists = any(
            v["name"] == current_vectorizer_name for v in vectorizers
        )

        if not vectorizer_exists or needs_new_vectorizer:
            actions.append(
                Action(
                    type=ActionType.SINK_VECTORIZER_SETUP,
                    description=f"Create new pgai vectorizer for {target_table} version {version_id}",
                    params={"table": target_table, "version_id": version_id},
                )
            )

        # 6. View Setup
        if (
            self.settings.sink_replica_table not in sink_state["views"]
            or current_hash != desired_hash
            or sink_state.get("view_target") != current_vectorizer_target
        ):
            actions.append(
                Action(
                    type=ActionType.SINK_VIEW_SETUP,
                    description=f"Setup search view {self.settings.sink_replica_table}",
                    params={
                        "config_hash": desired_hash,
                        "target_table": target_table,
                        "version_id": version_id,
                    },
                )
            )

        # 7. Cleanup old vectorizers
        # We only cleanup versions that are NEITHER the desired version NOR the one
        # currently being used by the search view. This ensures zero-downtime.
        current_live_view_target = sink_state.get("view_target")
        for v in vectorizers:
            if (
                v["target_table"] != current_vectorizer_target
                and v["target_table"] != current_live_view_target
            ):
                actions.append(
                    Action(
                        type=ActionType.SINK_TABLE_CLEANUP,
                        description=f"Cleanup orphaned vectorizer {v['id']} ({v['target_table']})",
                        params={
                            "id": v["id"],
                            "target_table": v["target_table"],
                        },
                    )
                )

        # 8. Recovery (Slot check)
        if self.settings.subscription_name not in source_state["slots"]:
            actions.append(
                Action(
                    type=ActionType.SINK_RECOVERY,
                    description="Perform hybrid recovery (missing slot)",
                    params={},
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
            if action.type == ActionType.SOURCE_SETUP:
                await setup_source(self.settings)

            elif action.type == ActionType.SINK_STATE_INIT:
                await setup_state_table(self.settings)

            elif action.type == ActionType.SINK_TABLE_EVOLVE:
                target_table = action.params["table"]
                if action.params["mode"] == "create":
                    from .database import ensure_sink_raw_table

                    await ensure_sink_raw_table(
                        self.settings, table_name=target_table
                    )
                else:
                    if target_table == "_replica_state":
                        source_types = {"config_hash": "TEXT"}
                    else:
                        source_types = await get_source_column_types(
                            self.settings
                        )

                    async with await get_sink_conn() as conn:
                        await conn.set_autocommit(True)
                        async with conn.cursor() as cur:
                            for col in action.params["columns"]:
                                dtype = source_types.get(col, "TEXT")
                                logger.info(
                                    f"Adding column {col} ({dtype}) to {target_table}"
                                )
                                await cur.execute(
                                    f"ALTER TABLE {target_table} ADD COLUMN {col} {dtype}"
                                )

            elif action.type == ActionType.SINK_VECTORIZER_SETUP:
                target_table = action.params["table"]
                version_id = action.params["version_id"]
                vectorizer_target = f"{target_table}_store_v{version_id}"
                # pgai deterministic naming for the embedding view
                vectorizer_view = vectorizer_target.replace(
                    "_store", "_embedding"
                )

                # Robust Cleanup: Drop auxiliary objects and vectorizer registration
                # if they exist. This prevents "relation already exists" errors and
                # ensures that pgai actually recreates the objects if they were
                # partially deleted.
                async with await get_sink_conn() as conn:
                    await conn.set_autocommit(True)
                    async with conn.cursor() as cur:
                        logger.info(
                            f"Pre-flight cleanup for vectorizer {vectorizer_target}..."
                        )
                        cleanup_sql = f"""
                        DO $$
                        DECLARE
                            live_view_target TEXT;
                        BEGIN
                            -- Ground Truth Check: Does the public search view depend on THIS version?
                            -- We query information_schema.view_table_usage to find the underlying table/view.
                            SELECT table_name INTO live_view_target 
                            FROM information_schema.view_table_usage 
                            WHERE view_name = '{self.settings.sink_replica_table}'
                            AND table_name IN ('{vectorizer_target}', '{vectorizer_view}')
                            LIMIT 1;

                            -- 1. If the public view depends on the version we are cleaning up,
                            -- we MUST drop it first to clear dependencies. This only happens
                            -- during retries of a failed deployment.
                            IF live_view_target IS NOT NULL THEN
                                EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident('{self.settings.sink_replica_table}') || ' CASCADE';
                            END IF;

                            -- 2. Now we can safely drop the orphaned/partial versioned objects.
                            -- This will NOT affect production if production is using an OLDER version.
                            IF EXISTS (
                                SELECT 1 FROM information_schema.tables 
                                WHERE table_schema = 'ai' AND table_name = 'vectorizer'
                            ) THEN
                                IF EXISTS (SELECT 1 FROM ai.vectorizer WHERE name = '{vectorizer_target}') THEN
                                    PERFORM ai.drop_vectorizer('{vectorizer_target}', drop_all => true);
                                END IF;
                            END IF;

                            EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident('{vectorizer_target}') || ' CASCADE';
                            EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident('{vectorizer_target}') || ' CASCADE';
                            EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident('{vectorizer_view}') || ' CASCADE';
                            EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident('{vectorizer_view}') || ' CASCADE';
                        END $$;
                        """
                        await cur.execute(cleanup_sql)

                from .database import setup_sink

                await setup_sink(
                    self.settings,
                    target_table=target_table,
                    vectorizer_target=vectorizer_target,
                )

                # Warm up if possible
                from .database import warm_up_from_cache

                await warm_up_from_cache(
                    self.settings, target_table, vectorizer_target
                )

            elif action.type == ActionType.SINK_VIEW_SETUP:
                from .database import atomic_view_swap

                await atomic_view_swap(
                    self.settings,
                    action.params["config_hash"],
                    target_table=action.params.get("target_table"),
                    vectorizer_target=f"{action.params['target_table']}_store_v{action.params['version_id']}",
                )

            elif action.type == ActionType.SINK_RECOVERY:
                source_state = await Inspector(self.settings).get_source_state()
                if self.settings.subscription_name in source_state["slots"]:
                    logger.info("Slot actually exists, skipping recovery.")
                    continue

                lsn = await create_placeholder_slot(self.settings)
                await update_replica_state(self.settings, lsn=lsn)
                await run_sql_catchup(self.settings)
                await find_and_fix_ghost_records(self.settings)

            elif action.type == ActionType.SINK_CACHE_SETUP:
                from .database import ensure_embedding_cache_table

                await ensure_embedding_cache_table(self.settings)

            elif action.type == ActionType.SINK_TABLE_CLEANUP:
                async with await get_sink_conn() as conn:
                    await conn.set_autocommit(True)
                    async with conn.cursor() as cur:
                        # 1. Drop pgai vectorizer
                        vectorizer_id = action.params["id"]
                        logger.info(f"Dropping pgai vectorizer {vectorizer_id}")
                        await cur.execute(
                            f"SELECT ai.drop_vectorizer({vectorizer_id}, drop_all => true)"
                        )

                        # 2. Drop auxiliary table
                        target_table = action.params["target_table"]
                        logger.info(f"Dropping auxiliary table {target_table}")
                        await cur.execute(
                            f"DROP TABLE IF EXISTS {target_table} CASCADE"
                        )
                        await cur.execute(
                            f"DROP VIEW IF EXISTS {target_table.replace('_store', '_embedding')} CASCADE"
                        )


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
        await self.applier.apply(actions)
        logger.info("Reconciliation complete.")
