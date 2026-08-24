"""
config/settings.py
==================
Centralised application configuration using Pydantic BaseSettings.
All values are read from environment variables (or a .env file).
Import via:  from config.settings import settings
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class SnowflakeSettings(BaseSettings):
    """Snowflake connection settings."""

    model_config = SettingsConfigDict(env_prefix="SNOWFLAKE_", extra="ignore")

    account: str = Field(..., description="Snowflake account identifier (e.g. xy12345.us-east-1)")
    user: str = Field(..., description="Snowflake username")
    password: str = Field(..., description="Snowflake password")
    database: str = Field(default="OBSERVABILITY_DB")
    warehouse: str = Field(default="COMPUTE_WH")
    role: str = Field(default="SYSADMIN")
    schema_: str = Field(default="OBSERVABILITY", alias="SNOWFLAKE_SCHEMA")

    # Snowflake layer schemas
    bronze_schema: str = Field(default="BRONZE")
    silver_schema: str = Field(default="SILVER")
    gold_schema: str = Field(default="GOLD")
    quarantine_schema: str = Field(default="QUARANTINE")

    @property
    def connection_params(self) -> dict:
        """Return dict compatible with snowflake.connector.connect()."""
        return {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "warehouse": self.warehouse,
            "role": self.role,
            "schema": self.schema_,
        }


class KafkaSettings(BaseSettings):
    """Kafka cluster settings."""

    model_config = SettingsConfigDict(env_prefix="KAFKA_", extra="ignore")

    bootstrap_servers: str = Field(default="kafka:9092")
    auto_offset_reset: str = Field(default="earliest")
    group_id_prefix: str = Field(default="observability-")

    @property
    def producer_config(self) -> dict:
        """Base config for Kafka producers."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 500,
        }

    @property
    def consumer_config(self) -> dict:
        """Base config for Kafka consumers."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False,
        }


class AirflowSettings(BaseSettings):
    """Airflow REST API settings (used by agents to trigger/retry DAGs)."""

    model_config = SettingsConfigDict(env_prefix="AIRFLOW__", extra="ignore")

    base_url: str = Field(default="http://airflow-webserver:8080")
    username: str = Field(default="admin")
    password: str = Field(default="admin")
    api_version: str = Field(default="v1")

    @property
    def api_base_url(self) -> str:
        return f"{self.base_url}/api/{self.api_version}"


class DataQualitySettings(BaseSettings):
    """Data quality check thresholds."""

    model_config = SettingsConfigDict(env_prefix="DQ_", extra="ignore")

    null_rate_threshold: float = Field(default=0.05, ge=0.0, le=1.0)
    duplicate_rate_threshold: float = Field(default=0.02, ge=0.0, le=1.0)
    type_mismatch_threshold: float = Field(default=0.01, ge=0.0, le=1.0)
    min_quality_score: float = Field(default=80.0, ge=0.0, le=100.0)
    gx_root_dir: str = Field(default="config/great_expectations")


class DriftSettings(BaseSettings):
    """Drift detection thresholds."""

    model_config = SettingsConfigDict(env_prefix="DRIFT_", extra="ignore")

    ks_pvalue_threshold: float = Field(default=0.05, ge=0.0, le=1.0)
    psi_threshold: float = Field(default=0.2, ge=0.0)
    reference_window_days: int = Field(default=30, ge=1)
    evidently_reports_dir: str = Field(default="/tmp/evidently_reports")


class CostSettings(BaseSettings):
    """Cost monitoring settings."""

    model_config = SettingsConfigDict(env_prefix="COST_", extra="ignore")

    snowflake_credit_price_usd: float = Field(default=3.0, ge=0.0)
    budget_daily_credits: float = Field(default=10.0, ge=0.0)
    budget_monthly_credits: float = Field(default=250.0, ge=0.0)
    alert_threshold_pct: int = Field(default=80, ge=0, le=100)


class AlertSettings(BaseSettings):
    """Alerting and notification settings."""

    model_config = SettingsConfigDict(extra="ignore")

    slack_webhook_url: str = Field(default="")
    alert_email_from: str = Field(default="alerts@example.com")
    alert_email_to: str = Field(default="team@example.com")
    smtp_host: str = Field(default="smtp.gmail.com")
    smtp_port: int = Field(default=587)
    smtp_user: str = Field(default="")
    smtp_password: str = Field(default="")


class AgentSettings(BaseSettings):
    """AI agent execution settings."""

    model_config = SettingsConfigDict(env_prefix="AGENT_", extra="ignore")

    max_retries: int = Field(default=3, ge=0)
    retry_delay_seconds: int = Field(default=60, ge=0)


class Settings(BaseSettings):
    """
    Master settings class.
    Composes all sub-settings and provides a single import point.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Application
    app_env: Literal["development", "staging", "production"] = Field(default="development")
    app_name: str = Field(default="DataObservability-AI")
    app_version: str = Field(default="0.1.0")
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(default="INFO")

    # Backend
    backend_host: str = Field(default="0.0.0.0")
    backend_port: int = Field(default=8000)
    backend_reload: bool = Field(default=True)
    backend_workers: int = Field(default=1)

    # Dashboard
    dashboard_port: int = Field(default=8501)
    backend_url: str = Field(default="http://backend:8000")

    @field_validator("app_env", mode="before")
    @classmethod
    def validate_env(cls, v: str) -> str:
        return v.lower()

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def is_development(self) -> bool:
        return self.app_env == "development"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached Settings instance. Use as a FastAPI dependency."""
    return Settings()


@lru_cache(maxsize=1)
def get_snowflake_settings() -> SnowflakeSettings:
    return SnowflakeSettings()


@lru_cache(maxsize=1)
def get_kafka_settings() -> KafkaSettings:
    return KafkaSettings()


@lru_cache(maxsize=1)
def get_dq_settings() -> DataQualitySettings:
    return DataQualitySettings()


@lru_cache(maxsize=1)
def get_drift_settings() -> DriftSettings:
    return DriftSettings()


@lru_cache(maxsize=1)
def get_cost_settings() -> CostSettings:
    return CostSettings()


@lru_cache(maxsize=1)
def get_alert_settings() -> AlertSettings:
    return AlertSettings()


@lru_cache(maxsize=1)
def get_agent_settings() -> AgentSettings:
    return AgentSettings()


# Convenience singleton — import this in most places
settings = get_settings()
snowflake_settings = get_snowflake_settings()
kafka_settings = get_kafka_settings()
dq_settings = get_dq_settings()
drift_settings = get_drift_settings()
cost_settings = get_cost_settings()
alert_settings = get_alert_settings()
agent_settings = get_agent_settings()
