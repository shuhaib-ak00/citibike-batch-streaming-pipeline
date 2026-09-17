"""Uji cepat kredensial & izin GCS + BigQuery dari dalam container.

Dipakai sebelum menjalankan DAG penuh, supaya masalah kredensial/izin
ketahuan dalam hitungan detik, bukan di tengah proses 1 juta baris.

Jalankan:
    docker compose exec airflow-scheduler python -m common.smoke_test

Tidak menulis data ke tabel produksi — hanya uji tulis/hapus objek
sementara di GCS dan query ringan ke BigQuery.
"""
from __future__ import annotations

import sys
import uuid

from common import bq_utils, gcs_utils
from common.config import (
    BUCKET_RAW,
    DATASET_RAW,
    LOCATION,
    PROJECT_ID,
    TABLE_TRIPS,
)

OK = "OK  "
FAIL = "FAIL"


def main() -> int:
    print("=" * 62)
    print("Smoke test kredensial GCP")
    print("=" * 62)
    print(f"  project      : {PROJECT_ID}")
    print(f"  location     : {LOCATION}")
    print(f"  bucket       : {BUCKET_RAW}")
    print(f"  dataset raw  : {DATASET_RAW}")
    print(f"  tabel trips  : {TABLE_TRIPS}")
    print("-" * 62)

    failures = 0

    # ---------- 1. GCS: tulis -> baca -> hapus ----------
    blob_name = f"_smoke_test/{uuid.uuid4().hex}.txt"
    payload = b"citibike smoke test"
    try:
        gcs_utils.bucket().blob(blob_name).upload_from_string(payload)
        back = gcs_utils.bucket().blob(blob_name).download_as_bytes()
        assert back == payload, "isi objek tidak sama saat dibaca kembali"
        gcs_utils.bucket().blob(blob_name).delete()
        print(f"{OK} GCS tulis/baca/hapus (gs://{BUCKET_RAW}/{blob_name})")
    except Exception as exc:  # noqa: BLE001
        failures += 1
        print(f"{FAIL} GCS: {type(exc).__name__}: {exc}")

    # ---------- 2. BigQuery: query ringan ----------
    try:
        val = bq_utils.query_scalar("SELECT 1")
        assert val == 1, f"hasil tak terduga: {val}"
        print(f"{OK} BigQuery query (SELECT 1 -> {val})")
    except Exception as exc:  # noqa: BLE001
        failures += 1
        print(f"{FAIL} BigQuery query: {type(exc).__name__}: {exc}")

    # ---------- 3. Tabel trips ada & bisa dibaca ----------
    try:
        cnt = bq_utils.query_scalar(f"SELECT COUNT(*) FROM `{TABLE_TRIPS}`")
        print(f"{OK} Tabel trips dapat dibaca (jumlah baris saat ini: {cnt})")
    except Exception as exc:  # noqa: BLE001
        failures += 1
        print(f"{FAIL} Baca tabel trips: {type(exc).__name__}: {exc}")

    # ---------- 4. Cek lokasi dataset (harus sama dgn GCP_LOCATION) ----------
    try:
        ds = bq_utils.client().get_dataset(f"{PROJECT_ID}.{DATASET_RAW}")
        loc = ds.location
        status = OK if str(loc).lower() == LOCATION.lower() else "WARN"
        print(f"{status} Lokasi dataset '{DATASET_RAW}': {loc} (diharapkan {LOCATION})")
    except Exception as exc:  # noqa: BLE001
        print(f"WARN Tidak bisa memverifikasi lokasi dataset: {type(exc).__name__}: {exc}")

    print("-" * 62)
    if failures:
        print(f"HASIL: {failures} pemeriksaan GAGAL")
        return 1
    print("HASIL: semua pemeriksaan lulus")
    return 0


if __name__ == "__main__":
    sys.exit(main())
