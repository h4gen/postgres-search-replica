import pytest
from pydantic import ValidationError
from pg_replica.config import (
    ReplicaConfig,
    SourceConfig,
    VectorizerConfig,
    FormattingConfig,
    SearchConfig,
    MirrorsConfig,
    MirrorTarget
)

def test_replicaconfig_strict_instantiation():
    """Verify strictly typed hierarchical instantiation."""
    config = ReplicaConfig(
        source=SourceConfig(
            table="products",
            primary_key="uuid",
            columns=["name", "description"],
            filter="active = true"
        ),
        vectorizer=VectorizerConfig(
            provider="openai",
            model="text-embedding-3-small",
            dimension=1536
        ),
        formatting=FormattingConfig(
            template="Title: $name",
            chunking_strategy="recursive_character_text_splitter"
        ),
        search=SearchConfig(
            profile="hybrid",
            target_engine="postgres"
        ),
        mirrors=MirrorsConfig(
            targets=[
                MirrorTarget(id="prod-qdrant", type="qdrant", url="http://qdrant:6333")
            ]
        )
    )
    
    assert config.source.table == "products"
    assert config.vectorizer.dimension == 1536
    assert config.search.profile == "hybrid"
    assert len(config.mirrors.targets) == 1

def test_replicaconfig_validation():
    """Verify validation rules prevent partial configs."""
    with pytest.raises(ValidationError):
        # Missing required field 'table'
        SourceConfig(columns=["foo"]) 
        
    # FormattingConfig now has a default template, so this no longer raises
    FormattingConfig()

def test_config_hash_determinism():
    """Verify get_config_hash is deterministic and aggregates sub-domains."""
    c1 = ReplicaConfig(
        source=SourceConfig(table="t1", columns=["c1"]),
        vectorizer=VectorizerConfig(),
        formatting=FormattingConfig(template="$c1"),
        search=SearchConfig(),
        mirrors=MirrorsConfig(targets=[])
    )
    
    c2 = ReplicaConfig(
        source=SourceConfig(table="t1", columns=["c1"]),
        vectorizer=VectorizerConfig(),
        formatting=FormattingConfig(template="$c1"),
        search=SearchConfig(),
        mirrors=MirrorsConfig(targets=[])
    )
    
    assert c1.get_config_hash() == c2.get_config_hash()
    
    # Modify Vectorizer -> Hash Changes
    c3 = c1.model_copy(deep=True)
    c3.vectorizer.dimension = 128
    assert c1.get_config_hash() != c3.get_config_hash()
    
    # Modify Search Profile -> Hash Changes (search is part of View Hash)
    c4 = c1.model_copy(deep=True)
    c4.search.profile = "hybrid"
    assert c1.get_config_hash() != c4.get_config_hash()
