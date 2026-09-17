"""DAG: ingest GBFS station_information (referensi stasiun).

**Kenapa DAG ini penting.** `station_information` adalah sumber kolom
`capacity` untuk `dim_station`. Selama tabel ini kosong, `dim_station.capacity`
selalu NULL sehingga occupancy rate tidak bisa dihitung — artinya mart risk
monitoring (empty/low/full) kehilangan dasar perhitungannya.

**Kenapa DAG terpisah, bukan bagian producer.** Sumbernya semi-statis (jarang
berubah), jadi cukup di-refresh berkala. Dipisah dari producer membuat tiap
bagian dapat dijalankan dan diverifikasi sendiri.

Alur:
    fetch station_information.json
      -> validasi envelope + tiap stasiun
      -> simpan JSON mentah & NDJSON ke GCS (arsip + bahan replay saat demo)
      -> gate kualitas payload
      -> replace penuh tabel referensi di BigQuery
      -> verifikasi jumlah baris & ketersediaan capacity

Idempotensi: tabel referensi ini **di-replace penuh** setiap run. Aman karena
tidak ada penulis lain, dan menghindari kebutuhan dedup di `dim_station`.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

import requests
from airflow.decorators import dag, task
from pydantic import ValidationError

from common import bq_utils, gcs_utils
from common.alert_utils import on_failure_alert
from common.config import (
    GCS_PREFIX_STATION_INFO,
    STATION_INFORMATION_URL,
    TABLE_STATION_INFO,
)
from streaming.schemas import StationInformation, StationInformationEnvelope

log = logging.getLogger(__name__)

HTTP_TIMEOUT_SECONDS = 20

# Proporsi stasiun yang gagal validasi di atas nilai ini dianggap masalah
# sistemik (bentuk payload sumber berubah), bukan noise yang boleh diabaikan.
REJECTION_ALERT_THRESHOLD_PCT = 5.0

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_failure_alert,
}


@dag(
    dag_id="citibike_ingest_station_info",
    description="Referensi: GBFS station_information -> GCS + BigQuery (sumber capacity dim_station)",
    schedule="@daily",          # sumber semi-statis; sekali sehari lebih dari cukup
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    # Tanpa on_failure_callback tingkat DAG; ringkasan kegagalan ditangani
    # citibike_watchdog_pipeline (lihat penjelasan di ingest_trips.py).
    default_args=default_args,
    tags=["citibike", "batch", "ingestion", "reference", "gbfs"],
    doc_md=__doc__,
)
def citibike_ingest_station_info() -> None:

    @task
    def fetch_and_stage() -> dict:
        """Ambil, validasi, lalu simpan ke GCS.

        Dua objek ditulis: JSON mentah (arsip & bahan replay) dan NDJSON
        baris siap-load.
        """
        resp = requests.get(
            STATION_INFORMATION_URL,
            timeout=HTTP_TIMEOUT_SECONDS,
            headers={"User-Agent": "citibike-pipeline/1.0"},
        )
        resp.raise_for_status()
        raw = resp.json()

        # ---- validasi envelope: struktur terluar harus benar ----
        try:
            envelope = StationInformationEnvelope.model_validate(raw)
        except ValidationError as exc:
            raise ValueError(
                f"Envelope station_information tidak valid "
                f"({exc.error_count()} masalah): {exc}"
            ) from exc

        ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        partition = ingested_at.date().isoformat()

        # ---- validasi per stasiun; yang rusak dihitung, bukan disembunyikan ----
        rows: list[dict] = []
        rejected: list[dict] = []
        for item in envelope.data.stations:
            try:
                station = StationInformation.model_validate(item)
            except ValidationError as exc:
                rejected.append(
                    {
                        "station_id": item.get("station_id"),
                        "error": exc.errors()[0].get("msg", "unknown"),
                    }
                )
                continue

            rows.append(
                {**station.model_dump(), "_ingested_at": ingested_at.isoformat()}
            )

        total = len(envelope.data.stations)
        rejected_pct = (len(rejected) / total * 100) if total else 0.0

        log.info(
            "station_information: %d stasiun, %d ditolak (%.2f%%)",
            len(rows),
            len(rejected),
            rejected_pct,
        )
        if rejected:
            log.warning("Contoh stasiun ditolak: %s", rejected[:5])

        # ---- arsip JSON mentah (berguna untuk replay bila API bermasalah) ----
        raw_blob = f"{GCS_PREFIX_STATION_INFO}/dt={partition}/station_information.json"
        gcs_utils.bucket().blob(raw_blob).upload_from_string(
            json.dumps(raw), content_type="application/json"
        )

        # ---- NDJSON baris siap-load ----
        ndjson = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
        ndjson_blob = (
            f"{GCS_PREFIX_STATION_INFO}/dt={partition}/station_information.ndjson"
        )
        gcs_utils.bucket().blob(ndjson_blob).upload_from_string(
            ndjson, content_type="application/x-ndjson"
        )
        log.info("Tersimpan: gs://%s/%s", gcs_utils.BUCKET_RAW, ndjson_blob)

        return {
            "partition": partition,
            "ndjson_uri": f"gs://{gcs_utils.BUCKET_RAW}/{ndjson_blob}",
            "n_stations": len(rows),
            "n_rejected": len(rejected),
            "rejected_pct": round(rejected_pct, 4),
            "last_updated": str(envelope.last_updated_dt),
        }

    @task
    def check_payload_quality(staged: dict) -> dict:
        """Hentikan pipeline bila terlalu banyak stasiun gagal validasi.

        Proporsi di atas ambang menandakan bentuk payload sumber berubah —
        masalah sistemik yang tidak boleh lolos ke dimensi.
        """
        pct = float(staged.get("rejected_pct") or 0.0)
        if pct > REJECTION_ALERT_THRESHOLD_PCT:
            raise ValueError(
                f"{staged['n_rejected']} dari "
                f"{staged['n_stations'] + staged['n_rejected']} stasiun gagal "
                f"validasi ({pct}% > ambang {REJECTION_ALERT_THRESHOLD_PCT}%). "
                f"Periksa apakah schema GBFS berubah."
            )
        return staged

    @task
    def load_reference(staged: dict) -> dict:
        """Replace penuh tabel referensi di BigQuery."""
        if staged["n_stations"] == 0:
            # Tanpa perlindungan ini, payload kosong akan menghapus data lama.
            raise ValueError(
                "Payload tidak memuat satu stasiun pun — tabel referensi tidak "
                "di-replace agar data lama tidak hilang."
            )

        rows = bq_utils.load_ndjson_replace(
            table_id=TABLE_STATION_INFO,
            source_uris=[staged["ndjson_uri"]],
            schema=bq_utils.STATION_INFO_SCHEMA,
        )
        return {**staged, "loaded_rows": rows}

    @task
    def verify_reference(loaded: dict) -> dict:
        """Pastikan isi tabel sesuai yang dimuat, dan capacity benar-benar terisi."""
        in_table = int(
            bq_utils.query_scalar(f"SELECT COUNT(*) FROM `{TABLE_STATION_INFO}`") or 0
        )
        has_capacity = int(
            bq_utils.query_scalar(
                f"SELECT COUNTIF(capacity IS NOT NULL) FROM `{TABLE_STATION_INFO}`"
            )
            or 0
        )

        summary = {
            "staged_rows": loaded["n_stations"],
            "loaded_rows": loaded["loaded_rows"],
            "rows_in_table": in_table,
            "rows_with_capacity": has_capacity,
            "rejected_rows": loaded["n_rejected"],
            "last_updated": loaded["last_updated"],
        }
        log.info("Verifikasi station_information: %s", summary)

        if in_table != loaded["n_stations"]:
            raise ValueError(
                f"Jumlah baris tabel ({in_table}) tidak sama dengan yang "
                f"dimuat ({loaded['n_stations']})."
            )
        if has_capacity == 0:
            # Tanpa capacity, occupancy tidak bisa dihitung — kegagalan nyata,
            # bukan sekadar kekurangan data.
            raise ValueError(
                "Tidak ada baris dengan capacity terisi — dim_station tidak "
                "akan bisa menghitung occupancy."
            )
        return summary

    staged = fetch_and_stage()
    checked = check_payload_quality(staged)
    loaded = load_reference(checked)
    verify_reference(loaded)


citibike_ingest_station_info()
