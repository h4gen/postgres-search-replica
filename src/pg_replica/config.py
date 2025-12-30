import os
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    source_url: str
    sink_url: str = "local"  # Default to local if not provided
    local_port: int = 54322  # Default port for local mode

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
        """Returns the actual connection string, resolving 'local' if necessary."""
        if self.sink_url == "local":
            # For local mode, we'll use a high port to avoid conflicts
            return f"postgresql://postgres@localhost:{self.local_port}/postgres"
        return self.sink_url

    @property
    def subscription_connection_url(self) -> str:
        """
        Returns the connection string used by the Sink DB to reach the Source DB.
        This can be different from source_url if running in Docker (e.g. 'source' vs 'localhost').
        """
        return os.environ.get("SUBSCRIPTION_SOURCE_URL", self.source_url)

    # Table names
    source_table: str = "products"
    sink_raw_table: str = "products"
    sink_replica_table: str = "products_replica"

    # Replication settings
    publication_name: str = "pub_products"
    publication_columns: list[str] = ["id", "name", "description"]
    publication_where: str | None = None
    max_slot_wal_keep_size_mb: int = 1024
    subscription_name: str = "sub_products"
    subscription_options: dict = {"streaming": "'on'"}
    batch_size: int = 50

    # Column mappings
    id_column: str = "id"
    content_column: str = "description"
    target_content_column: str = "transformed_description"
    embedding_column: str = "embedding"
    embedding_dimension: int = 768
    vectorizer_type: str = "ollama"

    # pgai Vectorizer Settings
    embedding_provider: str = "ollama"
    embedding_model: str = "nomic-embed-text"
    chunking_strategy: str = "recursive_character_text_splitter"

    # CHUNK DECORATION PRINCIPLE:
    # 1. The 'content_column' (description) is the "work column" that gets chunked.
    # 2. The 'formatting_template' allows you to "decorate" each chunk with metadata.
    # 3. '$chunk' is the mandatory placeholder for the piece of text being processed.
    # 4. Other columns (like '$name') provide global context for every chunk,
    #    ensuring the vector stays semantically linked to the product.
    formatting_template: str = "Product: $name Description: $chunk"

    # System settings
    notify_channel: str = "new_raw_data"

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.development"),
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()  # type: ignore[call-arg]
