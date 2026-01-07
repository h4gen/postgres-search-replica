import pytest
import asyncio
from pg_replica.config import (
    Settings, ReplicaConfig, SourceConfig, VectorizerConfig,
    FormattingConfig, SearchConfig, MirrorsConfig, MirrorTarget
)
from pg_replica.reconciler import Planner, ActionType

def test_planner_blocks_promotion_if_mirrors_lag():
    """
    Verify that PROMOTE_VIEW is NOT planned if mirror registry 
    lags behind outbox watermark for that version.
    """
    settings = Settings(
        source_url="postgresql://localhost/src",
        sink_url="postgresql://localhost/sink",
        replicas={
            "t1": ReplicaConfig(
                source=SourceConfig(table="table1", columns=["id", "name"]),
                vectorizer=VectorizerConfig(),
                formatting=FormattingConfig(template="$chunk"),
                search=SearchConfig(),
                mirrors=MirrorsConfig(targets=[MirrorTarget(id="m1", type="qdrant", url="http://q1")])
            )
        }
    )
    planner = Planner(settings)
    config = settings.replicas["t1"]
    v_id = config.get_version_id()
    expected_target = f"{config.source.table}_store_v{v_id}"

    source_state = {
        "publications": {f"pub_t1": {"tables": {"table1": {"rowfilter": None}}}},
        "slots": {f"sub_t1"},
    }
    
    # State where vectorizer is synced (0 pending)
    # BUT mirror is lagging (registry=5, outbox_watermark=10)
    sink_state = {
        "extensions": {"ai", "vector"},
        "tables": {
            "_replica_state": {"key", "config_hash"},
            "_embedding_cache": {"text_hash"},
            "_sink_outbox": {"id"},
            config.source.table: set(config.source.columns),
        },
        "views": {f"{config.source.table}_search"},
        "view_targets": {"t1": "old_target"},
        "replica_states": {"t1": {"config_hash": "old_hash"}},
        "triggers": {f"trg_outbox_t1_{v_id}"},
        "vectorizers": {
            config.source.table: [
                {"id": 2, "target_table": expected_target},
            ]
        },
        "vectorizer_statuses": {expected_target: 0},
        
        # New hand-shake state fields (Planned implementation)
        "outbox_watermarks": {v_id: 10},
        "mirror_progress": {("m1", "t1"): 5} # Lags behind 10
    }

    actions = planner.plan(source_state, sink_state)
    
    # It should NOT promote yet because mirror m1 hasn't reached 10
    assert not any(a.type == ActionType.SINK_VIEW_SETUP for a in actions), \
        "Should not promote while mirror is lagging"

    # Now simulate mirror catch-up
    sink_state["mirror_progress"][("m1", "t1")] = 10
    
    actions = planner.plan(source_state, sink_state)
    assert any(a.type == ActionType.SINK_VIEW_SETUP for a in actions), \
        "Should promote once mirror is caught up"
