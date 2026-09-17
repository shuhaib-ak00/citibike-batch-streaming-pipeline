"""Konfigurasi terpusat dari environment variable (.env).

Semua nilai diambil dari env agar tidak ada secret/hardcode di kode.
Di Airflow nilai ini di-inject oleh `env_file: .env` di docker-compose.
"""
from __future__ import annotations

import os
from pathlib import Path


def env(key: str, default: str | None = None, *, required: bool = False) -> str:
    """Ambil environment variable dengan validasi opsional."""
    value = os.getenv(key, default)
    if required and not value:
        raise RuntimeError(
            f"Environment variable '{key}' belum di-set. "
            f"Pastikan ada di .env dan container di-restart."
        )
    return value or ""


# ---------- GCP ----------
PROJECT_ID = env("GCP_PROJECT_ID", required=True)
LOCATION = env("GCP_LOCATION", "asia-southeast2")
BUCKET_RAW = env("GCS_BUCKET_RAW", required=True)
DATASET_RAW = env("BQ_DATASET_RAW", "nama_citibike_raw")
DATASET_DBT = env("BQ_DATASET_DBT", "nama_citibike")

# ---------- Path di dalam container ----------
RAW_DIR = Path(env("RAW_DATA_DIR", "/opt/airflow/data/raw"))
DBT_PROJECT_DIR = Path(env("DBT_PROJECT_DIR", "/opt/airflow/dbt"))
DBT_PROFILES_DIR = Path(env("DBT_PROFILES_DIR", "/opt/airflow/dbt/profiles"))

# ---------- Tabel target di BigQuery ----------
TABLE_TRIPS = f"{PROJECT_ID}.{DATASET_RAW}.trips"
TABLE_STATION_STATUS = f"{PROJECT_ID}.{DATASET_RAW}.station_status"
TABLE_DLQ = f"{PROJECT_ID}.{DATASET_RAW}.station_status_dlq"
TABLE_STATION_INFO = f"{PROJECT_ID}.{DATASET_RAW}.station_information"
TABLE_DQ_METRICS = f"{PROJECT_ID}.{DATASET_RAW}.dq_metrics"

# ---------- Dataset dbt per layer ----------
# Nama dibentuk oleh macro `generate_schema_name` (target.schema + '_' + layer).
DATASET_DBT_STAGING = f"{DATASET_DBT}_staging"
DATASET_DBT_INTERMEDIATE = f"{DATASET_DBT}_intermediate"
DATASET_DBT_MARTS = f"{DATASET_DBT}_marts"
DATASET_DBT_DASHBOARD = f"{DATASET_DBT}_dashboard"

TABLE_INT_TRIPS_DQ_SUMMARY = (
    f"{PROJECT_ID}.{DATASET_DBT_INTERMEDIATE}.int_trips_dq_summary"
)
TABLE_DASHBOARD_RISK = (
    f"{PROJECT_ID}.{DATASET_DBT_DASHBOARD}.station_risk_monitoring"
)
TABLE_STAGING_TRIPS = f"{PROJECT_ID}.{DATASET_DBT_STAGING}.stg_trips"
TABLE_STAGING_TRIPS_REJECTED = (
    f"{PROJECT_ID}.{DATASET_DBT_STAGING}.stg_trips_rejected"
)
TABLE_STAGING_STATION_STATUS = (
    f"{PROJECT_ID}.{DATASET_DBT_STAGING}.stg_station_status"
)
TABLE_STAGING_STATION_STATUS_REJECTED = (
    f"{PROJECT_ID}.{DATASET_DBT_STAGING}.stg_station_status_rejected"
)

# ---------- Prefix GCS (datalake) ----------
GCS_PREFIX_TRIPS = "raw/trips"
GCS_PREFIX_STATION_STATUS = "raw/station_status"
GCS_PREFIX_STATION_STATUS_REJECTED = "raw/station_status/_rejected"
GCS_PREFIX_STATION_INFO = "raw/station_information"

# ---------- Pola file sumber trip history ----------
TRIP_FILE_GLOB = "20*-citibike-tripdata_*.csv"

# ---------- Sumber GBFS ----------
GBFS_BASE_URL = env("GBFS_BASE_URL", "https://gbfs.citibikenyc.com/gbfs/en")
STATION_STATUS_URL = env(
    "STATION_STATUS_URL", "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
)
STATION_INFORMATION_URL = env(
    "STATION_INFORMATION_URL",
    "https://gbfs.citibikenyc.com/gbfs/en/station_information.json",
)
POLL_INTERVAL_SECONDS = int(env("POLL_INTERVAL_SECONDS", "90"))
