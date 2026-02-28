import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PostgresPublishConfig:
    enabled: bool
    host: str | None
    port: str
    database: str | None
    user: str | None
    password: str | None
    schema: str
    jdbc_driver: str
    jdbc_package: str | None
    jdbc_jar: str | None

    @classmethod
    def from_env(cls) -> "PostgresPublishConfig":
        enabled = _env_bool("PUBLISH_GOLD_TO_POSTGRES", True)
        return cls(
            enabled=enabled,
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            schema=os.getenv("POSTGRES_SCHEMA", "gold"),
            jdbc_driver=os.getenv("POSTGRES_JDBC_DRIVER", "org.postgresql.Driver"),
            jdbc_package=os.getenv("POSTGRES_JDBC_PACKAGE", "org.postgresql:postgresql:42.7.4"),
            jdbc_jar=os.getenv("POSTGRES_JDBC_JAR"),
        )

    def missing_required(self) -> list[str]:
        missing: list[str] = []
        if not self.host:
            missing.append("POSTGRES_HOST")
        if not self.database:
            missing.append("POSTGRES_DB")
        if not self.user:
            missing.append("POSTGRES_USER")
        if not self.password:
            missing.append("POSTGRES_PASSWORD")
        return missing

    def is_ready(self) -> bool:
        return self.enabled and not self.missing_required()

    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def spark_jars_packages(self) -> list[str]:
        if not self.enabled:
            return []
        if self.jdbc_jar:
            return []
        return [self.jdbc_package] if self.jdbc_package else []

    def spark_jars(self) -> list[str]:
        if not self.enabled:
            return []
        return [self.jdbc_jar] if self.jdbc_jar else []


@dataclass(frozen=True)
class PipelineConfig:
    input_dir: Path
    lake_dir: Path
    log_dir: Path
    app_name: str = "sales-pyspark-etl"
    postgres: PostgresPublishConfig | None = None


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}
