"""
config/constants.py
===================
Project-wide constants: Kafka topic names, Snowflake schema names,
pipeline status codes, agent names, severity levels, and metric types.

All constants are grouped into typed Enums or simple string constants.
Import selectively:
    from config.constants import KafkaTopic, SnowflakeSchema, AgentName
"""

from __future__ import annotations

from enum import Enum


# ─── Kafka Topics ─────────────────────────────────────────────────────────────

class KafkaTopic(str, Enum):
    """All Kafka topic names used across the platform."""

    # Ingestion
    RAW_DATA_EVENTS = "raw.data.events"

    # Observability events (published by agents)
    SCHEMA_EVENTS = "schema.events"
    QUALITY_EVENTS = "quality.events"
    DRIFT_EVENTS = "drift.events"
    COST_EVENTS = "cost.events"

    # Self-healing commands
    HEAL_COMMANDS = "heal.commands"


# ─── Snowflake Schemas ────────────────────────────────────────────────────────

class SnowflakeSchema(str, Enum):
    """Snowflake schema names for each layer."""

    BRONZE = "BRONZE"
    SILVER = "SILVER"
    GOLD = "GOLD"
    OBSERVABILITY = "OBSERVABILITY"
    QUARANTINE = "QUARANTINE"


# ─── Pipeline Status ──────────────────────────────────────────────────────────

class PipelineStatus(str, Enum):
    """Possible states for a pipeline run."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    HEALING = "healing"
    QUARANTINED = "quarantined"
    SKIPPED = "skipped"


# ─── Task Types ───────────────────────────────────────────────────────────────

class TaskType(str, Enum):
    """Airflow DAG task types."""

    INGEST = "ingest"
    TRANSFORM = "transform"
    QUALITY = "quality"
    DRIFT = "drift"
    COST = "cost"
    HEAL = "heal"
    ALERT = "alert"
    SCHEMA = "schema"


# ─── Agent Names ──────────────────────────────────────────────────────────────

class AgentName(str, Enum):
    """The five autonomous observability agents."""

    SCHEMA = "schema_agent"
    QUALITY = "quality_agent"
    DRIFT = "drift_agent"
    COST = "cost_agent"
    SELF_HEALING = "self_healing_agent"


# ─── Severity Levels ─────────────────────────────────────────────────────────

class Severity(str, Enum):
    """Severity levels for alerts and drift/quality issues."""

    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


# ─── Data Quality Metric Types ────────────────────────────────────────────────

class QualityMetricType(str, Enum):
    """Types of data quality metrics computed by the Quality Agent."""

    NULL_RATE = "null_rate"
    DUPLICATE_RATE = "duplicate_rate"
    TYPE_MISMATCH = "type_mismatch"
    OUT_OF_RANGE = "out_of_range"
    FRESHNESS_HOURS = "freshness_hours"
    COMPLETENESS = "completeness"


# ─── Drift Methods ────────────────────────────────────────────────────────────

class DriftMethod(str, Enum):
    """Statistical methods used for drift detection."""

    KS_TEST = "ks_test"              # Kolmogorov-Smirnov test (numeric)
    PSI = "psi"                      # Population Stability Index (numeric)
    JS_DIVERGENCE = "js_divergence"  # Jensen-Shannon divergence (numeric)
    CHI_SQUARED = "chi_squared"      # Chi-squared test (categorical)
    WASSERSTEIN = "wasserstein"      # Wasserstein distance (numeric)


# ─── Schema Change Types ──────────────────────────────────────────────────────

class SchemaChangeType(str, Enum):
    """Types of schema evolution events."""

    ADD_COLUMN = "ADD_COLUMN"
    DROP_COLUMN = "DROP_COLUMN"
    TYPE_CHANGE = "TYPE_CHANGE"
    RENAME_COLUMN = "RENAME_COLUMN"
    NO_CHANGE = "NO_CHANGE"


# ─── Healing Strategies ───────────────────────────────────────────────────────

class HealingStrategy(str, Enum):
    """Recovery strategies executed by the Self-Healing Agent."""

    RETRY = "retry"              # Re-run the failed task
    QUARANTINE = "quarantine"    # Move bad data to QUARANTINE schema
    REROUTE = "reroute"          # Send to DLQ for manual inspection
    ESCALATE = "escalate"        # Alert human operator
    SKIP = "skip"                # Skip the batch and continue
    ROLLBACK = "rollback"        # Rollback last dbt run


# ─── Failure Types ────────────────────────────────────────────────────────────

class FailureType(str, Enum):
    """Categories of pipeline failures for root-cause classification."""

    TRANSIENT = "transient"          # Network timeout, temporary Snowflake issue
    SCHEMA_MISMATCH = "schema_mismatch"  # Unexpected columns or type changes
    BAD_DATA = "bad_data"            # Data violates quality rules
    INFRASTRUCTURE = "infrastructure"  # Kafka down, Snowflake unreachable
    COST_LIMIT = "cost_limit"        # Budget threshold exceeded


# ─── Alert Channels ───────────────────────────────────────────────────────────

class AlertChannel(str, Enum):
    """Notification channels for alert dispatching."""

    SLACK = "slack"
    EMAIL = "email"
    AIRFLOW = "airflow"      # Airflow UI notification only


# ─── Source Types ─────────────────────────────────────────────────────────────

class SourceType(str, Enum):
    """Supported dataset source types."""

    CSV = "csv"
    JSON = "json"
    API = "api"
    S3 = "s3"


# ─── DAG IDs ─────────────────────────────────────────────────────────────────

class DagId(str, Enum):
    """Canonical Airflow DAG IDs."""

    INGESTION = "ingestion_dag"
    TRANSFORMATION = "transformation_dag"
    QUALITY_CHECK = "quality_check_dag"
    DRIFT_DETECTION = "drift_detection_dag"
    COST_MONITORING = "cost_monitoring_dag"
    SELF_HEALING = "self_healing_dag"


# ─── Numeric Constants ────────────────────────────────────────────────────────

# Airflow DAG retry settings
DEFAULT_MAX_RETRIES: int = 3
DEFAULT_RETRY_DELAY_MINUTES: int = 2
DEFAULT_RETRY_EXPONENTIAL_BASE: int = 2

# Quality scoring weights
QUALITY_WEIGHT_NULL: float = 0.30
QUALITY_WEIGHT_DUPLICATE: float = 0.20
QUALITY_WEIGHT_TYPE: float = 0.25
QUALITY_WEIGHT_FRESHNESS: float = 0.15
QUALITY_WEIGHT_RANGE: float = 0.10

# Snowflake cost
USD_PER_CREDIT: float = 3.0

# Kafka producer/consumer
KAFKA_POLL_TIMEOUT_SECONDS: float = 1.0
KAFKA_DELIVERY_TIMEOUT_MS: int = 30_000

# API pagination
API_DEFAULT_PAGE_SIZE: int = 50
API_MAX_PAGE_SIZE: int = 500

# Agent log table in Snowflake OBSERVABILITY schema
AGENT_SESSION_TABLE: str = "AGENT_SESSIONS"
PIPELINE_RUNS_TABLE: str = "PIPELINE_RUNS"
QUALITY_METRICS_TABLE: str = "QUALITY_METRICS"
DRIFT_METRICS_TABLE: str = "DRIFT_METRICS"
COST_RECORDS_TABLE: str = "COST_RECORDS"
