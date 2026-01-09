import hashlib
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Literal, Union, Annotated
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# --- CONFIG: Sources & Mirrors ---
class PostgresSourceConfig(BaseModel):
    type: Literal["postgres"] = "postgres"
    strategy: Literal["cdc", "polling"] = "cdc"
    connection_url: str

class LocalSourceConfig(BaseModel):
    type: Literal["local"] = "local"
    path: str
    uri_prefix: Optional[str] = None

class S3SourceConfig(BaseModel):
    type: Literal["s3"] = "s3"
    bucket: str
    prefix: str = ""
    region: Optional[str] = None
    endpoint_url: Optional[str] = None

SourceConfig = Annotated[
    Union[PostgresSourceConfig, LocalSourceConfig, S3SourceConfig],
    Field(discriminator="type")
]

class MirrorConfig(BaseModel):
    id: str
    type: Literal["qdrant", "pinecone"]
    config: Dict[str, Any]  # url, api_key, etc.

# --- 1. Ingest (Source) ---
class IngestConfig(BaseModel):
    source: str = "default"  # Reference to Settings.sources
    table: str
    columns: List[str]
    filter: Optional[str] = None
    p_key: str = "id"
    schema_name: str = "public"

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


class ParsingConfig(BaseModel):
    strategy: Literal["auto", "pdf", "docx", "markdown", "text"] = "auto"


class PipelineConfig(BaseModel):
    template: str # e.g. "Title: $title\n\n$content"
    content_column: str = "content"
    chunking: ChunkingConfig = Field(default_factory=ChunkingConfig)
    embedding: EmbeddingConfig
    parsing: ParsingConfig = Field(default_factory=ParsingConfig)

    @field_validator("template")
    @classmethod
    def validate_template(cls, v: str) -> str:
        if "$chunk" not in v:
            raise ValueError("Template must contain '$chunk' placeholder")
        return v


# --- 3. Storage (Persistence & Exports) ---
class BranchConfig(BaseModel):
    name: str                   
    pipeline: PipelineConfig


class PostgresStoreConfig(BaseModel):
    """Defines the internal Postgres View Schema."""
    profile: Literal["vector", "hybrid"] = "vector" 
    retention: str = "forever"


class StorageConfig(BaseModel):
    postgres: PostgresStoreConfig = Field(default_factory=PostgresStoreConfig)
    branches: List[BranchConfig] = []
    mirrors: List[str] = []  # References to Settings.mirrors keys


# --- 4. Serve (Runtime API) ---
class SearchProfile(BaseModel):
    mode: Literal["vector", "hybrid", "keyword"]
    weights: Optional[Dict[str, float]] = None
    target_branch: str = "main"
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
        relevant_config = {
            "ingest": self.ingest.model_dump(),
            "pipeline": self.pipeline.model_dump(),
            "storage_profile": self.storage.postgres.profile,
        }
        config_json = json.dumps(relevant_config, sort_keys=True, default=str)
        return hashlib.sha256(config_json.encode()).hexdigest()

    def get_version_id(self) -> str:
        return self.get_config_hash()[:8]


# --- SYSTEM: Runtime Environment ---
class Settings(BaseSettings):
    # Registry Patterns
    sources: Dict[str, SourceConfig] = Field(default_factory=dict)
    mirrors: Dict[str, MirrorConfig] = Field(default_factory=dict)

    sink_url: str = "local"
    local_port: int = 54322
    source_managed_by_admin: bool = False

    pipelines: Dict[str, SearchPipeline] = Field(default_factory=dict)
    
    # Legacy Support
    source_url: Optional[str] = None

    @model_validator(mode="after")
    def validate_pipelines_and_sources(self) -> "Settings":
        # 1. Pipeline Validation
        new_pipelines = {}
        for k, v in self.pipelines.items():
            if isinstance(v, dict):
                new_pipelines[k] = SearchPipeline(**v)
            else:
                new_pipelines[k] = v
        self.pipelines = new_pipelines
        
        # 2. Legacy Source Support
        # If 'default' source is missing but source_url is provided (env var), create it.
        if "default" not in self.sources and self.source_url:
            self.sources["default"] = PostgresSourceConfig(connection_url=self.source_url)
            
        return self

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

    # Global replication/safety settings
    max_slot_wal_keep_size_mb: int = 1024
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
