import hashlib
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# --- 1. Ingest (Source) ---
class IngestConfig(BaseModel):
    table: str
    columns: List[str]
    filter: Optional[str] = None  # SQL where clause, e.g. "active = true"
    p_key: str = "id"
    schema_name: str = "public"
    # Future: distinct source connection ID

    @model_validator(mode="after")
    def ensure_pkey_in_columns(self) -> "IngestConfig":
        if self.p_key not in self.columns:
            self.columns.append(self.p_key)
        return self


# --- 2. Pipeline (Transform) ---
class ChunkingConfig(BaseModel):
    strategy: Literal["recursive_character", "markdown", "sentence"] = "recursive_character"
    size: int = 1500
    overlap: int = 200
    separator: Optional[str] = None


class EmbeddingConfig(BaseModel):
    provider: Literal["openai", "ollama", "bedrock", "voyageai"]
    model: str
    dimension: int
    api_key_name: Optional[str] = None # Env var name for API key


class PipelineConfig(BaseModel):
    template: str # e.g. "Title: $title\n\n$content"
    content_column: str = "content" # The main text column for change tracking
    chunking: ChunkingConfig = Field(default_factory=ChunkingConfig)
    embedding: EmbeddingConfig

    @field_validator("template")
    @classmethod
    def validate_template(cls, v: str) -> str:
        if "$chunk" not in v:
            raise ValueError("Template must contain '$chunk' placeholder")
        return v


# --- 3. Storage (Persistence & Exports) ---
class BranchConfig(BaseModel):
    """SearchOps: A shadow index for experimentation (e.g. v2 model)."""
    name: str                   
    pipeline: PipelineConfig    # Override main pipeline settings (different model/chunking)


class MirrorConfig(BaseModel):
    """Export target (Downstream)."""
    id: str
    type: Literal["qdrant", "pinecone"]
    config: Dict[str, Any]      # url, prefix, api_key


class PostgresStoreConfig(BaseModel):
    """Defines the internal Postgres View Schema."""
    # hybrid = adds tsvector column. vector = vector only.
    profile: Literal["vector", "hybrid"] = "vector" 
    retention: str = "forever" # Placeholder for future data lifecycle


class StorageConfig(BaseModel):
    postgres: PostgresStoreConfig = Field(default_factory=PostgresStoreConfig)
    branches: List[BranchConfig] = [] # "SearchOps" feature
    mirrors: List[MirrorConfig] = []  # Export targets


# --- 4. Serve (Runtime API) ---
class SearchProfile(BaseModel):
    """Declarative Search Profile (Runtime Tuning)."""
    mode: Literal["vector", "hybrid", "keyword"]
    weights: Optional[Dict[str, float]] = None # { "vector": 0.7, "text": 0.3 } for RRF
    target_branch: str = "main"         # Can point to a branch!
    limit: int = 10


class ServeConfig(BaseModel):
    enabled: bool = True
    default_profile: str = "default"
    profiles: Dict[str, SearchProfile] = {
        "default": SearchProfile(mode="hybrid")
    }


# --- ROOT: The Search Pipeline ---
class SearchPipeline(BaseModel):
    ingest: IngestConfig
    pipeline: PipelineConfig
    storage: StorageConfig = Field(default_factory=StorageConfig)
    serve: ServeConfig = Field(default_factory=ServeConfig)
    
    active: bool = True

    def get_config_hash(self) -> str:
        """
        Generates a SHA256 hash of the 'Structural' configuration.
        Changes here require a REBUILD (new version).
        
        Explicitly EXCLUDES:
        - serve (Runtime only)
        - storage.mirrors (Downstream only)
        - active (Runtime state)
        """
        relevant_config = {
            "ingest": self.ingest.model_dump(),
            "pipeline": self.pipeline.model_dump(),
            # Only storing profile affects the View definition
            "storage_profile": self.storage.postgres.profile,
        }
        # Sort keys for deterministic hashing
        config_json = json.dumps(relevant_config, sort_keys=True, default=str)
        return hashlib.sha256(config_json.encode()).hexdigest()

    def get_version_id(self) -> str:
        """Returns a short version ID based on the config hash."""
        return self.get_config_hash()[:8]


# --- SYSTEM: Runtime Environment ---
class Settings(BaseSettings):
    source_url: str
    sink_url: str = "local"  # Default to local if not provided
    local_port: int = 54322  # Default port for local mode

    # Enterprise Source Integration
    source_managed_by_admin: bool = False

    # Multi-Table Configuration
    pipelines: Dict[str, SearchPipeline] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_pipelines(self) -> "Settings":
        # Ensure all pipelines are SearchPipeline objects
        new_pipelines = {}
        for k, v in self.pipelines.items():
            if isinstance(v, dict):
                new_pipelines[k] = SearchPipeline(**v)
            else:
                new_pipelines[k] = v
        self.pipelines = new_pipelines
        return self

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

    # Global replication/safety settings
    max_slot_wal_keep_size_mb: int = 1024
    subscription_options: dict = {"streaming": "'on'"}
    batch_size: int = 50

    # System settings
    notify_channel: str = "new_raw_data"

    # Observability
    observability_host: str = "0.0.0.0"
    observability_port: int = 8000

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.development"),
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()  # type: ignore[call-arg]
