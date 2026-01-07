from tests.factories import SearchPipelineFactory
from src.pg_replica.config_v2 import SearchPipeline

def test_pipeline_factory_smoke():
    """Verify that the factory produces valid SearchPipeline objects."""
    pipeline = SearchPipelineFactory.create(
        table_name="users",
        hybrid=True
    )
    
    # Assert Root Types
    assert isinstance(pipeline, SearchPipeline)
    assert pipeline.active is True
    
    # Assert Nested Semantic Structures
    assert pipeline.ingest.table == "users"
    assert pipeline.pipeline.embedding.provider == "openai"
    assert pipeline.storage.postgres.profile == "hybrid"
    assert pipeline.serve.profiles["default"].mode == "hybrid"

def test_config_hash_stability():
    """Verify get_config_hash excludes Serve config."""
    p1 = SearchPipelineFactory.create(table_name="t1")
    hash1 = p1.get_config_hash()
    
    # Change Serve config (runtime tuning)
    p1.serve.profiles["default"].limit = 100
    hash2 = p1.get_config_hash()
    
    assert hash1 == hash2, "Changing ServeConfig MUST NOT change the config hash"
    
    # Change Pipeline config (requires rebuild)
    p1.pipeline.template = "New Template: $chunk"
    hash3 = p1.get_config_hash()
    
    assert hash1 != hash3, "Changing PipelineConfig MUST change the config hash"
