from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    source_url: str
    sink_url: str

    # Table names
    source_table: str = "users"
    sink_raw_table: str = "users"
    sink_replica_table: str = "users_replica"

    # Replication settings
    publication_name: str = "pub_users"
    publication_columns: list[str] = ["id", "email"]
    publication_where: str | None = None
    max_slot_wal_keep_size_mb: int = 1024
    subscription_name: str = "sub_users"
    subscription_options: dict = {"streaming": "'on'"}
    batch_size: int = 50

    # Column mappings
    id_column: str = "id"
    content_column: str = "email"
    target_content_column: str = "transformed_email"
    embedding_column: str = "embedding"
    embedding_dimension: int = 3
    vectorizer_type: str = "dummy"

    # System settings
    notify_channel: str = "new_raw_data"

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.development"),
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()  # type: ignore[call-arg]
