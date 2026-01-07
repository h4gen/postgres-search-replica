import hashlib
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TableConfig(BaseModel):
    """Configuration for a single source-to-sink replication target."""
    
    # Source Settings
    source_table: str
    publication_columns: List[str] = Field(default_factory=list)
    publication_where: Optional[str] = None
    
    # Sink Settings (Optional, with defaults based on source_table)
    sink_raw_table: Optional[str] = None
    sink_replica_table: Optional[str] = None
 
    def model_post_init(self, __context) -> None:
        if self.sink_raw_table is None:
            ## NEVER APPEND SOMETHING HERE> NEEDED FOR LOGICAL REPLICATION IN POSTGRES
            self.sink_raw_table = self.source_table
        if self.sink_replica_table is None:
            self.sink_replica_table = f"{self.source_table}_search"
        
        # Ensure ID and Content columns are in publication_columns
        if self.id_column not in self.publication_columns:
            self.publication_columns.append(self.id_column)
        if self.content_column not in self.publication_columns:
            self.publication_columns.append(self.content_column)
        
        # Add columns from formatting_template
        import re
        template_vars = re.findall(r"\$(\w+)", self.formatting_template)
        for var in template_vars:
            if var != "chunk" and var not in self.publication_columns:
                self.publication_columns.append(var)
        
        # FINAL VALIDATION: pgai requires $chunk
        if "$chunk" not in self.formatting_template:
            raise ValueError(f"formatting_template for table {self.source_table} must contain '$chunk' placeholder")
 
    # Column Mapping
    id_column: str = "id"
    content_column: str = "description"
    target_content_column: str = "chunk"
    embedding_column: str = "embedding"
    
    # Search & Transformation
    search_profile: str = "vector"  # options: vector, hybrid, sparse
    search_engine: str = "postgres"  # options: postgres, qdrant, pinecone
    embedding_provider: str = "ollama"
    embedding_model: str = "nomic-embed-text"
    embedding_dimension: int = 768
    chunking_strategy: str = "recursive_character_text_splitter"
    formatting_template: str = "Product: $name Description: $chunk"
    
    # Multicast Mirror Settings
    mirrors: List[Dict[str, Any]] = Field(default_factory=list)

    # Deployment State
    active: bool = True

    def get_config_hash(self) -> str:
        """Generates a SHA256 hash of the search-relevant configuration for THIS table."""
        relevant_config = {
            "publication_columns": sorted(self.publication_columns),
            "publication_where": self.publication_where,
            "embedding_provider": self.embedding_provider,
            "embedding_model": self.embedding_model,
            "chunking_strategy": self.chunking_strategy,
            "formatting_template": self.formatting_template,
            "embedding_dimension": self.embedding_dimension,
            "search_profile": self.search_profile,
        }
        config_json = json.dumps(relevant_config, sort_keys=True)
        return hashlib.sha256(config_json.encode()).hexdigest()

    def get_version_id(self) -> str:
        """Returns a short version ID based on the config hash."""
        return self.get_config_hash()[:8]


from pydantic import model_validator
from .config_v2 import SearchPipeline

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
