import hashlib
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SourceConfig(BaseModel):
    """Infrastructure and Data Contract."""
    table: str
    primary_key: str = "id"
    content_column: str = "description"
    columns: List[str] = Field(default_factory=list)
    filter: Optional[str] = None
    max_slot_wal_keep_size_mb: int = 1024


class VectorizerConfig(BaseModel):
    """Mathematical Model Definition."""
    provider: str = "ollama"
    model: str = "nomic-embed-text"
    dimension: int = 768


class FormattingConfig(BaseModel):
    """Content Preparation."""
    template: str = "$chunk"
    target_content_column: str = "transformed_content"
    chunking_strategy: str = "recursive_character_text_splitter"


class SearchConfig(BaseModel):
    """Query Time Behavior."""
    profile: str = "vector"  # options: vector, hybrid
    target_engine: str = "postgres"
    embedding_column: str = "embedding"


class MirrorTarget(BaseModel):
    """External sink definition."""
    id: str
    type: str
    url: str
    prefix: str = ""


class MirrorsConfig(BaseModel):
    """External Distribution."""
    targets: List[MirrorTarget] = Field(default_factory=list)


class ReplicaConfig(BaseModel):
    """The Root Configuration Object (Replaces TableConfig)."""
    source: SourceConfig
    vectorizer: VectorizerConfig = VectorizerConfig()
    formatting: FormattingConfig = FormattingConfig(template="$chunk")
    search: SearchConfig = SearchConfig()
    mirrors: MirrorsConfig = MirrorsConfig()
    active: bool = True

    def get_config_hash(self) -> str:
        """Generates a SHA256 hash of the search-relevant configuration."""
        relevant_config = {
            "source": {
                "columns": sorted(self.source.columns),
                "filter": self.source.filter,
            },
            "vectorizer": self.vectorizer.model_dump(),
            "formatting": self.formatting.model_dump(),
            "search": self.search.model_dump(),
        }
        config_json = json.dumps(relevant_config, sort_keys=True)
        return hashlib.sha256(config_json.encode()).hexdigest()

    def get_version_id(self) -> str:
        """Returns a short version ID based on the config hash."""
        return self.get_config_hash()[:8]


class Settings(BaseSettings):
    source_url: str
    sink_url: str = "local"
    local_port: int = 54322
    source_managed_by_admin: bool = False

    # RENAMED: tables -> replicas
    replicas: Dict[str, ReplicaConfig] = Field(default_factory=dict)

    # Storage paths for local mode
    base_dir: Path = Path(
        os.environ.get(
            "PG_REPLICA_DIR",
            Path.home() / ".local" / "share" / "pg-search-replica",
        )
    )

    @property
    def data_dir(self) -> Path:
        return self.base_dir / "data"

    @property
    def resolved_sink_url(self) -> str:
        if self.sink_url == "local":
            return f"postgresql://postgres@localhost:{self.local_port}/postgres"
        return self.sink_url

    @property
    def subscription_connection_url(self) -> str:
        return os.environ.get("SUBSCRIPTION_SOURCE_URL", self.source_url)

    # Global safety settings
    subscription_options: dict = {"streaming": "'on'"}
    batch_size: int = 50
    notify_channel: str = "new_raw_data"
    observability_host: str = "0.0.0.0"
    observability_port: int = 8000

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.development"),
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()  # type: ignore[call-arg]
