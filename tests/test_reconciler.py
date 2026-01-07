import pytest
from pg_replica.config import (
    Settings, ReplicaConfig, SourceConfig, VectorizerConfig,
    FormattingConfig, SearchConfig, MirrorsConfig, MirrorTarget
)
from pg_replica.reconciler import Planner, ActionType


def test_planner_no_drift():
    settings = Settings(
        source_url="postgresql://localhost/src",
        sink_url="postgresql://localhost/sink",
        replicas={
            "t1": ReplicaConfig(
                source=SourceConfig(table="table1"),
                vectorizer=VectorizerConfig(),
                formatting=FormattingConfig(template="$chunk"),
                search=SearchConfig(),
                mirrors=MirrorsConfig()
            )
        }
    )
    planner = Planner(settings)
    config = settings.replicas["t1"]
    v_id = config.get_version_id()
    raw_table = config.source.table
    search_view = f"{raw_table}_search"

    source_state = {
        "publications": {f"pub_t1": {"tables": {"table1": {"rowfilter": None}}}},
        "slots": {f"sub_t1"},
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            "_embedding_cache": {"text_hash"},
            raw_table: set(config.source.columns),
            "_sink_outbox": set(),
        },
        "views": {search_view},
        "view_targets": {"t1": f"{raw_table}_store_v{v_id}"},
        "replica_states": {"t1": {"config_hash": config.get_config_hash()}},
        "triggers": {f"trg_outbox_t1_{v_id}"},
        "vectorizers": {
            raw_table: [
                {
                    "id": 1,
                    "name": f"{raw_table}_store_v{v_id}",
                    "target_table": f"{raw_table}_store_v{v_id}",
                }
            ]
        },
        "vectorizer_statuses": {},
    }

    actions = planner.plan(source_state, sink_state)
    assert len(actions) == 0


def test_planner_missing_column():
    settings = Settings(
        source_url="postgresql://localhost/src",
        sink_url="postgresql://localhost/sink",
        replicas={
            "t1": ReplicaConfig(
                source=SourceConfig(
                    table="table1",
                    columns=["id", "name", "description", "price"]
                ),
                vectorizer=VectorizerConfig(),
                formatting=FormattingConfig(),
                search=SearchConfig(),
                mirrors=MirrorsConfig()
            )
        }
    )
    planner = Planner(settings)
    config = settings.replicas["t1"]
    v_id = config.get_version_id()
    raw_table = config.source.table
    search_view = f"{raw_table}_search"

    source_state = {
        "publications": {f"pub_t1": {"tables": {"table1": {"rowfilter": None}}}},
        "slots": {f"sub_t1"},
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            "_embedding_cache": {"text_hash"},
            raw_table: {"id", "name", "description"},
            "_sink_outbox": set(),
        },
        "views": {search_view},
        "view_targets": {"t1": f"{raw_table}_store_v{v_id}"},
        "replica_states": {"t1": {"config_hash": config.get_config_hash()}},
        "triggers": {f"trg_outbox_t1_{v_id}"},
        "vectorizers": {raw_table: []},
        "vectorizer_statuses": {},
    }

    actions = planner.plan(source_state, sink_state)
    assert any(a.type == ActionType.SINK_TABLE_EVOLVE for a in actions)


def test_planner_model_change_triggers_view_swap():
    settings = Settings(
        source_url="postgresql://localhost/src",
        sink_url="postgresql://localhost/sink",
        replicas={
            "t1": ReplicaConfig(
                source=SourceConfig(table="table1"),
                vectorizer=VectorizerConfig(model="new-model"),
                formatting=FormattingConfig(),
                search=SearchConfig(),
                mirrors=MirrorsConfig()
            )
        }
    )
    planner = Planner(settings)
    config = settings.replicas["t1"]
    raw_table = config.source.table
    search_view = f"{raw_table}_search"

    source_state = {
        "publications": {f"pub_t1": {"tables": {"table1": {"rowfilter": None}}}},
        "slots": {f"sub_t1"},
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            "_embedding_cache": {"text_hash"},
            raw_table: set(config.source.columns),
        },
        "views": {search_view},
        "view_targets": {"t1": raw_table + "_store_vold"},
        "replica_states": {"t1": {"config_hash": "old_hash"}},
        "vectorizers": {
            raw_table: [
                {
                    "id": 1,
                    "name": "old_vec",
                    "target_table": raw_table + "_store_vold",
                }
            ]
        },
        "vectorizer_statuses": {raw_table + "_store_v" + config.get_version_id(): 0},
    }

    actions = planner.plan(source_state, sink_state)
    assert any(a.type == ActionType.SINK_VIEW_SETUP for a in actions)


def test_planner_missing_slot_triggers_recovery():
    settings = Settings(
        source_url="postgresql://localhost/src",
        sink_url="postgresql://localhost/sink",
        replicas={
            "t1": ReplicaConfig(
                source=SourceConfig(table="table1"),
                vectorizer=VectorizerConfig(),
                formatting=FormattingConfig(),
                search=SearchConfig(),
                mirrors=MirrorsConfig()
            )
        }
    )
    planner = Planner(settings)
    config = settings.replicas["t1"]
    v_id = config.get_version_id()
    raw_table = config.source.table
    search_view = f"{raw_table}_search"

    source_state = {
        "publications": {f"pub_t1": {"tables": {"table1": {"rowfilter": None}}}},
        "slots": set(),  # Missing slot
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            "_embedding_cache": {"text_hash"},
            raw_table: set(config.source.columns),
        },
        "views": {search_view},
        "view_targets": {"t1": f"{raw_table}_store_v{v_id}"},
        "replica_states": {"t1": {"config_hash": config.get_config_hash()}},
        "vectorizers": {
            raw_table: [
                {
                    "id": 1,
                    "name": f"{raw_table}_store_v{v_id}",
                    "target_table": f"{raw_table}_store_v{v_id}",
                }
            ]
        },
        "vectorizer_statuses": {},
    }

    actions = planner.plan(source_state, sink_state)
    assert any(a.type == ActionType.SINK_RECOVERY for a in actions)


def test_planner_deferred_swap():
    """Verify that promotion is skipped if target is NOT synced."""
    settings = Settings(
        source_url="postgresql://localhost/src",
        sink_url="postgresql://localhost/sink",
        replicas={
            "t1": ReplicaConfig(
                source=SourceConfig(table="table1"),
                vectorizer=VectorizerConfig(model="new-model"),
                formatting=FormattingConfig(),
                search=SearchConfig(),
                mirrors=MirrorsConfig(),
                active=True
            )
        }
    )
    planner = Planner(settings)
    config = settings.replicas["t1"]
    v_id = config.get_version_id()
    raw_table = config.source.table
    search_view = f"{raw_table}_search"
    expected_target = f"{raw_table}_store_v{v_id}"

    source_state = {
        "publications": {f"pub_t1": {"tables": {"table1": {"rowfilter": None}}}},
        "slots": {f"sub_t1"},
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            raw_table: set(config.source.columns),
        },
        "views": {search_view}, # View exists
        "view_targets": {"t1": "old_target"}, # Points to old target
        "replica_states": {"t1": {"config_hash": "old_hash"}},
        "vectorizers": {
            raw_table: [
                {"id": 1, "target_table": "old_target"},
                {"id": 2, "target_table": expected_target},
            ]
        },
        # CRITICAL: Pending items > 0
        "vectorizer_statuses": {expected_target: 100},
    }

    actions = planner.plan(source_state, sink_state)
    
    # Deployment should ENSURE vectorizer exists (no setup needed if in list)
    # But it should NOT plan a SINK_VIEW_SETUP because pending_items > 0
    assert not any(a.type == ActionType.SINK_VIEW_SETUP for a in actions)
    
    # Now simulate synced
    sink_state["vectorizer_statuses"][expected_target] = 0
    actions = planner.plan(source_state, sink_state)
    assert any(a.type == ActionType.SINK_VIEW_SETUP for a in actions)
