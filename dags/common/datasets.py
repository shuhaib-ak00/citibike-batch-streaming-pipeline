"""Airflow Dataset (data-aware scheduling).

Dipakai agar DAG transformasi dbt berjalan **setelah** data raw benar-benar
tersedia — bukan sekadar pada jam yang ditebak. Ini menghilangkan kelas bug
"dbt jalan duluan lalu marts basi".

Dataset adalah URI logis; isinya tidak diakses, hanya dipakai sebagai
penanda ketergantungan antar-DAG.
"""
from __future__ import annotations

from airflow.datasets import Dataset

from common.config import (
    DATASET_DBT,
    DATASET_DBT_STAGING,
    DATASET_RAW,
    PROJECT_ID,
)

# Dihasilkan oleh DAG ingestion batch (citibike_ingest_trips)
RAW_TRIPS = Dataset(f"bigquery://{PROJECT_ID}/{DATASET_RAW}/trips")

# Tersedia bila nanti perlu memicu DAG dari sisi streaming. Saat ini belum
# dipakai: consumer tidak memancarkan dataset, dan rantai streaming dijadwalkan
# per jam lewat citibike_transform_streaming.
RAW_STATION_STATUS = Dataset(f"bigquery://{PROJECT_ID}/{DATASET_RAW}/station_status")

# Ditulis oleh model dbt staging trip (dipakai bila perlu memicu DAG
# lanjutan setelah staging selesai, bukan setelah raw siap).
STAGING_TRIPS = Dataset(f"bigquery://{PROJECT_ID}/{DATASET_DBT_STAGING}/stg_trips")

__all__ = ["RAW_TRIPS", "RAW_STATION_STATUS", "STAGING_TRIPS"]
