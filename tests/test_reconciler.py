import pytest
from pg_replica.config import Settings
from pg_replica.config_v2 import SearchPipeline, IngestConfig, PipelineConfig, StorageConfig, EmbeddingConfig, ChunkingConfig, PostgresStoreConfig
from pg_replica.reconciler import Planner, ActionType


def test_planner_no_drift():
    settings = Settings(
        source_url="postgresql://localhost/src",
        sink_url="postgresql://localhost/sink",
        pipelines={
            "t1": SearchPipeline(
                ingest=IngestConfig(table="table1", columns=["id", "content"]),
                pipeline=PipelineConfig(
                    template="$chunk", 
                    embedding=EmbeddingConfig(provider="ollama", model="nomic-embed-text", dimension=768)
                )
            )
        }
    )
    planner = Planner(settings)
    config = settings.pipelines["t1"]
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
            config.ingest.table: set(config.ingest.columns),
            "_sink_outbox": set(),
        },
        "views": {f"{config.ingest.table}_search"},
        "view_targets": {"t1": f"{config.ingest.table}_store_v{v_id}"},
        "replica_states": {"t1": {"config_hash": config.get_config_hash()}},
        "triggers": {f"trg_outbox_t1_{v_id}"},
        "vectorizers": {
            config.ingest.table: [
                {
                    "id": 1,
                    "name": f"{config.ingest.table}_store_v{v_id}",
                    "target_table": f"{config.ingest.table}_store_v{v_id}",
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
        pipelines={
            "t1": SearchPipeline(
                ingest=IngestConfig(table="table1", columns=["id", "name", "description", "price"]),
                pipeline=PipelineConfig(
                    template="$chunk", 
                    embedding=EmbeddingConfig(provider="ollama", model="nomic-embed-text", dimension=768)
                )
            )
        }
    )
    planner = Planner(settings)
    config = settings.pipelines["t1"]
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
            config.ingest.table: {"id", "name", "description"},
            "_sink_outbox": set(),
        },
        "views": {f"{config.ingest.table}_search"},
        "view_targets": {"t1": f"{config.ingest.table}_store_v{v_id}"},
        "replica_states": {"t1": {"config_hash": config.get_config_hash()}},
        "triggers": {f"trg_outbox_t1_{v_id}"},
        "vectorizers": {config.ingest.table: []},
        "vectorizer_statuses": {},
    }

    actions = planner.plan(source_state, sink_state)
    assert any(a.type == ActionType.SINK_TABLE_EVOLVE for a in actions)


def test_planner_model_change_triggers_view_swap():
    # Simulate a drift by having the in-memory config differ from the 'state'
    # Here we define the "new" desire state
    settings = Settings(
        source_url="postgresql://localhost/src",
        sink_url="postgresql://localhost/sink",
        pipelines={
            "t1": SearchPipeline(
                ingest=IngestConfig(table="table1", columns=["id", "content"]),
                pipeline=PipelineConfig(
                    template="$chunk", 
                    embedding=EmbeddingConfig(provider="ollama", model="new-model", dimension=768)
                )
            )
        }
    )
    planner = Planner(settings)
    config = settings.pipelines["t1"]

    source_state = {
        "publications": {f"pub_t1": {"tables": {"table1": {"rowfilter": None}}}},
        "slots": {f"sub_t1"},
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            "_embedding_cache": {"text_hash"},
            config.ingest.table: set(config.ingest.columns),
        },
        "views": {f"{config.ingest.table}_search"},
        "view_targets": {"t1": config.ingest.table + "_store_vold"},
        "replica_states": {"t1": {"config_hash": "old_hash"}},
        "vectorizers": {
            config.ingest.table: [
                {
                    "id": 1,
                    "name": "old_vec",
                    "target_table": config.ingest.table + "_store_vold",
                }
            ]
        },
        "vectorizer_statuses": {config.ingest.table + "_store_v" + config.get_version_id(): 0},
    }

    actions = planner.plan(source_state, sink_state)
    assert any(a.type == ActionType.SINK_VIEW_SETUP for a in actions)


def test_planner_missing_slot_triggers_recovery():
    settings = Settings(
        source_url="postgresql://localhost/src",
        sink_url="postgresql://localhost/sink",
        pipelines={
            "t1": SearchPipeline(
                ingest=IngestConfig(table="table1", columns=["id", "content"]),
                pipeline=PipelineConfig(
                    template="$chunk", 
                    embedding=EmbeddingConfig(provider="ollama", model="nomic-embed-text", dimension=768)
                )
            )
        }
    )
    planner = Planner(settings)
    config = settings.pipelines["t1"]
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
            config.ingest.table: set(config.ingest.columns),
        },
        "views": {f"{config.ingest.table}_search"},
        "view_targets": {"t1": f"{config.ingest.table}_store_v{v_id}"},
        "replica_states": {"t1": {"config_hash": config.get_config_hash()}},
        "vectorizers": {
            config.ingest.table: [
                {
                    "id": 1,
                    "name": f"{config.ingest.table}_store_v{v_id}",
                    "target_table": f"{config.ingest.table}_store_v{v_id}",
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
        pipelines={
            "t1": SearchPipeline(
                ingest=IngestConfig(table="table1", columns=["id", "content"]),
                pipeline=PipelineConfig(
                    template="$chunk", 
                    embedding=EmbeddingConfig(provider="ollama", model="new-model", dimension=768)
                ),
                active=True
            )
        }
    )
    planner = Planner(settings)
    config = settings.pipelines["t1"]
    v_id = config.get_version_id()
    expected_target = f"{config.ingest.table}_store_v{v_id}"

    source_state = {
        "publications": {f"pub_t1": {"tables": {"table1": {"rowfilter": None}}}},
        "slots": {f"sub_t1"},
    }
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            config.ingest.table: set(config.ingest.columns),
        },
        "views": {f"{config.ingest.table}_search"}, # View exists
        "view_targets": {"t1": "old_target"}, # Points to old target
        "replica_states": {"t1": {"config_hash": "old_hash"}},
        "vectorizers": {
            config.ingest.table: [
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
