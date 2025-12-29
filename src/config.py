from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    source_url: str
    sink_url: str
    replication_source_url: str | None = None
    publication_name: str = "pub_users"
    subscription_name: str = "sub_users"
    batch_size: int = 50

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.development"), env_file_encoding="utf-8", extra="ignore"
    )


settings = Settings()  # type: ignore[missing-argument]
