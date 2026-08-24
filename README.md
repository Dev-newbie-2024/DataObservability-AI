# DataObservability-AI

> **Design and Implementation of a Dataset-Agnostic, Cost-Aware and Self-Healing Data Observability Platform**
>
> VTU Major Project — Dept. of ISE, 2025–2026

---

## Overview

A production-grade data observability platform that ingests **any structured dataset** (CSV/JSON), processes it through a **Medallion architecture** (Bronze → Silver → Gold) on Snowflake, continuously monitors **data quality** and **data drift**, tracks **cloud costs**, and **self-heals** from failures — all without manual intervention.

## Architecture

```
Data Sources (CSV/JSON)
        │
   [Kafka Ingest]
        │
   Snowflake BRONZE  ──► dbt ──► SILVER ──► dbt ──► GOLD
        │
   ┌────┴──────────────────────────────┐
   │        Observability Engine        │
   │  Great Expectations  │  Evidently  │
   │  Quality Agent       │  Drift Agent│
   │  Cost Agent          │  Heal Agent │
   └────────────┬──────────────────────┘
                │
         FastAPI Backend
                │
        Streamlit Dashboard
```

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Apache Kafka (KRaft) |
| Orchestration | Apache Airflow 2.9 |
| Transformation | dbt-core + Snowflake |
| Data Warehouse | Snowflake |
| Quality | Great Expectations |
| Drift Detection | Evidently AI |
| AI Agents | Python (rule-based + statistical) |
| API | FastAPI |
| Dashboard | Streamlit |
| Containerization | Docker + Docker Compose |
| CI/CD | GitHub Actions |

## Project Structure

```
DataObservability-AI/
├── backend/          # FastAPI REST API
├── airflow/          # Airflow DAGs, operators, sensors, hooks
├── ai_agents/        # Five autonomous observability agents
├── dashboard/        # Streamlit observability UI (6 pages)
├── dbt/              # dbt transformation project (Bronze→Silver→Gold)
├── docker/           # Dockerfiles for all services
├── tests/            # Unit, integration, E2E tests
├── docs/             # Project documentation
├── config/           # App config, GX suites, Kafka topics
├── docker-compose.yml
├── pyproject.toml
└── requirements.txt
```

## Quick Start

### Prerequisites

- Docker Desktop (≥ 4.25) with at least 8 GB RAM allocated
- A Snowflake account (free trial works)
- Git

### 1. Clone & Configure

```bash
git clone https://github.com/<your-org>/DataObservability-AI.git
cd DataObservability-AI

# Copy environment template and fill in your Snowflake credentials
cp .env.example .env
```

Edit `.env` with your Snowflake credentials:

```env
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=OBSERVABILITY_DB
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=SYSADMIN
```

### 2. Start All Services

```bash
docker compose up -d
```

### 3. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Streamlit Dashboard | http://localhost:8501 | — |
| FastAPI Docs | http://localhost:8000/docs | — |
| Airflow UI | http://localhost:8080 | admin / admin |
| Kafka UI | http://localhost:8090 | — |

### 4. Verify Health

```bash
# Check all containers running
docker compose ps

# View logs
docker compose logs -f backend
docker compose logs -f dashboard
```

### 5. Initialize Snowflake Schema

```bash
# Run migration scripts (after services are up)
docker compose exec backend python -m backend.db.run_migrations
```

## Development

### Install Dependencies (Local)

```bash
# Using uv (recommended)
pip install uv
uv pip install -e ".[dev]"

# Or using pip
pip install -r requirements.txt
```

### Run Tests

```bash
# Unit tests only (no external services)
pytest tests/unit/ -v

# Integration tests (requires docker compose up)
pytest tests/integration/ -v --timeout=120

# All tests
pytest -v
```

### Code Quality

```bash
# Lint
ruff check .

# Type check
mypy backend/ ai_agents/ --ignore-missing-imports

# Format
ruff format .
```

### dbt Commands

```bash
cd dbt/

# Compile (validates SQL without running)
dbt compile --profiles-dir ../config/

# Run transformations
dbt run --profiles-dir ../config/

# Run tests
dbt test --profiles-dir ../config/
```

## AI Agents

Five autonomous agents monitor and heal the platform:

| Agent | Trigger | Action |
|-------|---------|--------|
| **Schema Agent** | New dataset upload | Auto-detects schema, creates Snowflake tables |
| **Quality Agent** | After each ingestion | Runs GX validation, scores quality 0–100 |
| **Drift Agent** | Daily | Runs Evidently reports, classifies drift severity |
| **Cost Agent** | Daily 4 AM | Tracks Snowflake credits, forecasts 30-day cost |
| **Self-Healing Agent** | On pipeline failure | Classifies failure, retries/quarantines/escalates |

## Kafka Topics

| Topic | Purpose | Partitions |
|-------|---------|------------|
| `raw.data.events` | Raw ingested records | 6 |
| `schema.events` | Schema change notifications | 2 |
| `quality.events` | DQ check results | 4 |
| `drift.events` | Drift detection results | 2 |
| `cost.events` | Cost monitoring alerts | 2 |
| `heal.commands` | Self-healing instructions | 2 |

## Environment Variables

See [`.env.example`](.env.example) for the full list of required environment variables.

## CI/CD

GitHub Actions workflows:

- **`ci.yml`** — Runs on every push: lint, type check, unit tests, dbt compile
- **`integration_tests.yml`** — Runs on PR to `main`: spins up Docker stack, runs integration tests
- **`deploy.yml`** — Runs on merge to `main`: builds and pushes Docker images to GHCR

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -m 'feat: add my feature'`)
4. Push and open a Pull Request

## License

MIT License — see [LICENSE](LICENSE)

---

**VTU Project** | Dept. of ISE | 2025–2026
