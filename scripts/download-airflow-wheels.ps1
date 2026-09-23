<#
.SYNOPSIS
    Download all Airflow image wheels for Linux x86_64 on the Windows host,
    where the network works, so the Docker build never contacts PyPI.

.DESCRIPTION
    Runs pip download targeting manylinux_2_28 / linux x86_64 so the wheels
    are installable inside the apache/airflow:2.9.3-python3.12 container.
    The wheels are saved to airflow/wheels/ which is COPYed into the image.

.USAGE
    From the repo root:
        .\scripts\download-airflow-wheels.ps1

    Then rebuild:
        docker compose build --no-cache airflow-init airflow-webserver airflow-scheduler
        docker compose up -d
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$WheelDir = Join-Path $PSScriptRoot "..\airflow\wheels"

Write-Host "==> Cleaning $WheelDir" -ForegroundColor Cyan
if (Test-Path $WheelDir) { Remove-Item -Recurse -Force $WheelDir }
New-Item -ItemType Directory -Force $WheelDir | Out-Null

# Target the exact Python / platform that the base image runs
$PythonVersion = "cp312"
$Platform      = "manylinux2014_x86_64"

Write-Host "==> Downloading wheels for $PythonVersion / $Platform ..." -ForegroundColor Cyan

pip download `
    --dest              $WheelDir `
    --python-version    "3.12" `
    --platform          $Platform `
    --implementation    "cp" `
    --abi               $PythonVersion `
    --only-binary       ":all:" `
    --prefer-binary `
    apache-airflow-providers-snowflake `
    apache-airflow-providers-apache-kafka `
    "snowflake-connector-python>=3.10.0" `
    "confluent-kafka>=2.4.0" `
    dbt-core `
    dbt-snowflake `
    "pydantic>=2.7.0" `
    "pydantic-settings>=2.3.0" `
    "python-dotenv>=1.0.0" `
    "structlog>=24.2.0" `
    "tenacity>=8.3.0" `
    "httpx>=0.27.0"

if ($LASTEXITCODE -ne 0) {
    Write-Error "pip download failed — check your network and retry."
    exit 1
}

$Count = (Get-ChildItem $WheelDir -Filter "*.whl").Count
Write-Host "==> Done. $Count wheels saved to airflow\wheels\" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  docker compose build --no-cache airflow-init airflow-webserver airflow-scheduler"
Write-Host "  docker compose up -d"
