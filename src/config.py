from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    source_url: str
    sink_url: str
    publication_name: str = "pub_users"
    publication_columns: list[str] = ["id", "email"]
    publication_where: str | None = None
    max_slot_wal_keep_size_mb: int = 1024
    subscription_name: str = "sub_users"
    subscription_options: dict = {"streaming": "'on'"}
    batch_size: int = 50

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.development"),
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()  # type: ignore[missing-argument]
