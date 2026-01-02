import pytest
from pg_replica.config import Settings, TableConfig
from pg_replica.reconciler import Planner, ActionType


def test_planner_no_drift():
    settings = Settings(
        source_url="postgresql://localhost/src",
        sink_url="postgresql://localhost/sink",
        tables={
            "t1": TableConfig(source_table="table1")
        }
    )
    planner = Planner(settings)
    config = settings.tables["t1"]
    v_id = config.get_version_id()

    source_state = {
        "publications": {f"pub_t1": {"tables": {"table1": {"rowfilter": None}}}},
        "slots": {f"sub_t1"},
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            "_embedding_cache": {"text_hash"},
            config.sink_raw_table: set(config.publication_columns),
        },
        "views": {config.sink_replica_table},
        "view_targets": {"t1": f"{config.sink_raw_table}_store_v{v_id}"},
        "replica_states": {"t1": {"config_hash": config.get_config_hash()}},
        "vectorizers": {
            config.sink_raw_table: [
                {
                    "id": 1,
                    "name": f"{config.sink_raw_table}_store_v{v_id}",
                    "target_table": f"{config.sink_raw_table}_store_v{v_id}",
                }
            ]
        },
    }

    actions = planner.plan(source_state, sink_state)
    assert len(actions) == 0


def test_planner_missing_column():
    settings = Settings(
        source_url="postgresql://localhost/src",
        sink_url="postgresql://localhost/sink",
        tables={
            "t1": TableConfig(
                source_table="table1",
                publication_columns=["id", "name", "description", "price"]
            )
        }
    )
    planner = Planner(settings)
    config = settings.tables["t1"]
    v_id = config.get_version_id()

    source_state = {
        "publications": {f"pub_t1": {"tables": {"table1": {"rowfilter": None}}}},
        "slots": {f"sub_t1"},
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            "_embedding_cache": {"text_hash"},
            config.sink_raw_table: {"id", "name", "description"},
        },
        "views": {config.sink_replica_table},
        "view_targets": {"t1": f"{config.sink_raw_table}_store_v{v_id}"},
        "replica_states": {"t1": {"config_hash": config.get_config_hash()}},
        "vectorizers": {config.sink_raw_table: []},
    }

    actions = planner.plan(source_state, sink_state)
    assert any(a.type == ActionType.SINK_TABLE_EVOLVE for a in actions)


def test_planner_model_change_triggers_view_swap():
    settings = Settings(
        source_url="postgresql://localhost/src",
        sink_url="postgresql://localhost/sink",
        tables={
            "t1": TableConfig(source_table="table1", embedding_model="new-model")
        }
    )
    planner = Planner(settings)
    config = settings.tables["t1"]

    source_state = {
        "publications": {f"pub_t1": {"tables": {"table1": {"rowfilter": None}}}},
        "slots": {f"sub_t1"},
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            "_embedding_cache": {"text_hash"},
            config.sink_raw_table: set(config.publication_columns),
        },
        "views": {config.sink_replica_table},
        "view_targets": {"t1": config.sink_raw_table + "_store_vold"},
        "replica_states": {"t1": {"config_hash": "old_hash"}},
        "vectorizers": {
            config.sink_raw_table: [
                {
                    "id": 1,
                    "name": "old_vec",
                    "target_table": config.sink_raw_table + "_store_vold",
                }
            ]
        },
    }

    actions = planner.plan(source_state, sink_state)
    assert any(a.type == ActionType.SINK_VIEW_SETUP for a in actions)


def test_planner_missing_slot_triggers_recovery():
    settings = Settings(
        source_url="postgresql://localhost/src",
        sink_url="postgresql://localhost/sink",
        tables={
            "t1": TableConfig(source_table="table1")
        }
    )
    planner = Planner(settings)
    config = settings.tables["t1"]
    v_id = config.get_version_id()

    source_state = {
        "publications": {f"pub_t1": {"tables": {"table1": {"rowfilter": None}}}},
        "slots": set(),  # Missing slot
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            "_embedding_cache": {"text_hash"},
            config.sink_raw_table: set(config.publication_columns),
        },
        "views": {config.sink_replica_table},
        "view_targets": {"t1": f"{config.sink_raw_table}_store_v{v_id}"},
        "replica_states": {"t1": {"config_hash": config.get_config_hash()}},
        "vectorizers": {
            config.sink_raw_table: [
                {
                    "id": 1,
                    "name": f"{config.sink_raw_table}_store_v{v_id}",
                    "target_table": f"{config.sink_raw_table}_store_v{v_id}",
                }
            ]
        },
    }

    actions = planner.plan(source_state, sink_state)
    assert any(a.type == ActionType.SINK_RECOVERY for a in actions)
