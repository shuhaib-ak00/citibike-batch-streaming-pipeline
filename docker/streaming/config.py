"""Konfigurasi pipeline streaming (semua dari environment)."""
from __future__ import annotations

import os
from pathlib import Path


def env(key: str, default: str | None = None, *, required: bool = False) -> str:
    value = os.getenv(key, default)
    if required and not value:
        raise RuntimeError(
            f"Environment variable '{key}' belum di-set. Pastikan ada di .env."
        )
    return value or ""


# ---------- Kafka ----------
BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_SNAPSHOT = env("KAFKA_TOPIC_STATION_SNAPSHOT", "citibike.station_status.snapshot")
TOPIC_DLQ = env("KAFKA_TOPIC_STATION_DLQ", "citibike.station_status.dlq")
CONSUMER_GROUP = env("KAFKA_CONSUMER_GROUP", "citibike-station-status-writer")

# ---------- GBFS ----------
STATION_STATUS_URL = env(
    "STATION_STATUS_URL", "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
)
STATION_INFORMATION_URL = env(
    "STATION_INFORMATION_URL",
    "https://gbfs.citibikenyc.com/gbfs/en/station_information.json",
)
POLL_INTERVAL_SECONDS = int(env("POLL_INTERVAL_SECONDS", "90"))
HTTP_TIMEOUT_SECONDS = int(env("HTTP_TIMEOUT_SECONDS", "20"))

# ---------- GCP ----------
PROJECT_ID = env("GCP_PROJECT_ID", required=True)
LOCATION = env("GCP_LOCATION", "asia-southeast2")
BUCKET_RAW = env("GCS_BUCKET_RAW", required=True)
DATASET_RAW = env("BQ_DATASET_RAW", "shuhaib_citibike_raw")

TABLE_STATION_STATUS = f"{PROJECT_ID}.{DATASET_RAW}.station_status"
TABLE_DLQ = f"{PROJECT_ID}.{DATASET_RAW}.station_status_dlq"

GCS_PREFIX_STATION_STATUS = "raw/station_status"
GCS_PREFIX_STATION_STATUS_REJECTED = "raw/station_status/_rejected"
GCS_PREFIX_STATION_INFO = "raw/station_information"

# ---------- Perilaku consumer ----------
# Tulis massal jauh lebih efisien daripada satu load job per pesan.
CONSUMER_BATCH_SIZE = int(env("CONSUMER_BATCH_SIZE", "5000"))
# Batas tunggu bila batch belum penuh, agar data tidak mengendap lama.
CONSUMER_FLUSH_SECONDS = int(env("CONSUMER_FLUSH_SECONDS", "30"))

# ---------- Path ----------
REFERENCE_DIR = Path(env("REFERENCE_DIR", "/opt/app/data/reference"))