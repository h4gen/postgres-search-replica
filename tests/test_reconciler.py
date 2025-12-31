import pytest
from pg_replica.config import Settings
from pg_replica.reconciler import Planner, ActionType

def test_planner_no_drift():
    settings = Settings(source_url="postgresql://localhost/src", sink_url="postgresql://localhost/sink")
    planner = Planner(settings)
    
    source_state = {
        "publications": {settings.publication_name: {"rowfilter": None}},
        "slots": {settings.subscription_name},
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            "_embedding_cache": {"text_hash"},
            settings.sink_raw_table: set(settings.publication_columns),
        },
        "views": {settings.sink_replica_table},
        "view_target": settings.sink_raw_table,
        "replica_state": {"config_hash": settings.get_config_hash()},
        "vectorizers": {settings.sink_raw_table: [{"id": 1}]},
    }
    
    actions = planner.plan(source_state, sink_state)
    assert len(actions) == 0

def test_planner_missing_column():
    settings = Settings(
        source_url="postgresql://localhost/src", 
        sink_url="postgresql://localhost/sink",
        publication_columns=["id", "name", "description", "price"]
    )
    planner = Planner(settings)
    
    source_state = {
        "publications": {settings.publication_name: {"rowfilter": None}},
        "slots": {settings.subscription_name},
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            "_embedding_cache": {"text_hash"},
            settings.sink_raw_table: {"id", "name", "description"},
        },
        "views": {settings.sink_replica_table},
        "replica_state": {"config_hash": settings.get_config_hash()},
        "vectorizers": {settings.sink_raw_table},
    }
    
    actions = planner.plan(source_state, sink_state)
    assert any(a.type == ActionType.SINK_TABLE_EVOLVE for a in actions)
    assert any("price" in str(a.params.get("columns", [])) for a in actions)

def test_planner_model_change_triggers_view_swap():
    settings = Settings(
        source_url="postgresql://localhost/src", 
        sink_url="postgresql://localhost/sink",
        embedding_model="new-model"
    )
    planner = Planner(settings)
    
    source_state = {
        "publications": {settings.publication_name: {"rowfilter": None}},
        "slots": {settings.subscription_name},
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            "_embedding_cache": {"text_hash"},
            settings.sink_raw_table: set(settings.publication_columns),
        },
        "views": {settings.sink_replica_table},
        "replica_state": {"config_hash": "old_hash"},
        "vectorizers": {settings.sink_raw_table},
    }
    
    actions = planner.plan(source_state, sink_state)
    assert any(a.type == ActionType.SINK_VIEW_SETUP for a in actions)

def test_planner_missing_slot_triggers_recovery():
    settings = Settings(source_url="postgresql://localhost/src", sink_url="postgresql://localhost/sink")
    planner = Planner(settings)
    
    source_state = {
        "publications": {settings.publication_name: {"rowfilter": None}},
        "slots": set(), # Missing slot
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            "_embedding_cache": {"text_hash"},
            settings.sink_raw_table: set(settings.publication_columns),
        },
        "views": {settings.sink_replica_table},
        "view_target": settings.sink_raw_table,
        "replica_state": {"config_hash": settings.get_config_hash()},
        "vectorizers": {settings.sink_raw_table: [{"id": 1}]},
    }
    
    actions = planner.plan(source_state, sink_state)
    assert any(a.type == ActionType.SINK_RECOVERY for a in actions)

