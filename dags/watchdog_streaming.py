"""DAG: watchdog streaming.

Pipeline streaming bisa berhenti tanpa error apa pun: Airflow sehat, container
masih "Up", tetapi tidak ada data masuk (laptop sleep, consumer mati, skema
GBFS berubah, load ke BigQuery gagal). Tidak satu pun memicu
``on_failure_callback`` karena tidak ada task yang gagal.

Karena itu watchdog memeriksa GEJALA, bukan proses:

    check_freshness          data terbaru di warehouse terlalu tua? (utama)
    check_consumer_liveness  consumer masih menulis ke datalake?
    check_dlq_growth         payload mulai rusak beruntun?
    check_staging_rejection  proporsi karantina melonjak?

Dua yang pertama dipisah agar letak masalah bisa dipersempit: consumer hidup
tetapi warehouse tetap tua berarti masalahnya di load, bukan di consumer.

Watchdog tidak menghentikan apa pun — hanya mengamati lalu memberi tahu.
Ambang diatur lewat variabel lingkungan; lihat ``.env.example``.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from common import bq_utils, gcs_utils
from common.alert_utils import alert_streaming
from common.config import (
    GCS_PREFIX_STATION_STATUS,
    TABLE_DLQ,
    TABLE_STAGING_STATION_STATUS,
    TABLE_STAGING_STATION_STATUS_REJECTED,
    TABLE_STATION_STATUS,
)

log = logging.getLogger(__name__)

# ---------- Ambang pemeriksaan ----------
# Data di warehouse dianggap basi bila snapshot terbaru lebih tua dari ini.
FRESHNESS_THRESHOLD_MINUTES = int(
    os.getenv("STREAMING_FRESHNESS_THRESHOLD_MINUTES", "10")
)
# Datalake dianggap berhenti menerima tulisan bila objek terbaru lebih tua.
CONSUMER_LIVENESS_MINUTES = int(os.getenv("CONSUMER_LIVENESS_MINUTES", "10"))
# Jumlah baris DLQ dalam jendela ini yang dianggap sebagai lonjakan.
DLQ_WINDOW_MINUTES = int(os.getenv("DLQ_WINDOW_MINUTES", "30"))
DLQ_ALERT_ROWS = int(os.getenv("DLQ_ALERT_ROWS", "200"))
# Proporsi karantina staging yang dianggap masalah sistemik.
REJECTION_THRESHOLD_PCT = float(os.getenv("STREAMING_REJECTION_THRESHOLD_PCT", "5"))

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

DAG_ID = "citibike_watchdog_streaming"


@dag(
    dag_id=DAG_ID,
    description="Pantau kesehatan pipeline streaming (freshness, consumer, DLQ, karantina)",
    # Tiap 5 menit: cukup rapat untuk memenuhi target deteksi <=10 menit,
    # tanpa membebani BigQuery dengan query yang tidak perlu.
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # Tanpa flag ini, DAG baru ter-pause dan watchdog tidak akan pernah
    # berjalan pada clone segar — kegagalan yang sunyi, persis kondisi yang
    # ingin dicegah oleh DAG ini sendiri.
    is_paused_upon_creation=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args=default_args,
    tags=["citibike", "streaming", "monitoring", "alert"],
    doc_md=__doc__,
)
def citibike_watchdog_streaming() -> None:

    # --------------------------------------------------------
    # 1. Freshness end-to-end
    # --------------------------------------------------------
    @task
    def check_freshness() -> dict:
        """Periksa umur snapshot terbaru di warehouse.

        Ini pemeriksaan paling penting: mencakup seluruh rantai dari
        producer sampai load ke BigQuery. Bila data terlalu tua, apa pun
        penyebabnya, tim ops perlu tahu.
        """
        sql = f"""
            SELECT
                MAX(snapshot_timestamp) AS latest,
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(snapshot_timestamp), SECOND)
                    AS age_seconds,
                COUNT(DISTINCT snapshot_timestamp) AS total_snapshot
            FROM `{TABLE_STATION_STATUS}`
        """
        row = next(iter(bq_utils.client().query(sql).result()), None)
        if row is None or row["latest"] is None:
            hasil = {"status": "EMPTY", "detail": "Tabel station_status kosong."}
            log.error("Watchdog freshness: %s", hasil)
            alert_streaming(
                check="freshness",
                title="Streaming belum pernah menerima data",
                detail=hasil["detail"],
                observed=0,
                threshold=f"{FRESHNESS_THRESHOLD_MINUTES} menit",
                severity="CRITICAL",
            )
            return hasil

        age_seconds = int(row["age_seconds"] or 0)
        age_minutes = round(age_seconds / 60, 1)

        if age_seconds > FRESHNESS_THRESHOLD_MINUTES * 60:
            # Letak masalah dipersempit supaya pesan alert langsung berguna.
            petunjuk = (
                "Consumer kemungkinan mati, atau laptop/komputer sleep sehingga "
                "seluruh container berhenti."
            )
            log.error(
                "Data streaming basi %.1f menit (ambang %d menit).",
                age_minutes, FRESHNESS_THRESHOLD_MINUTES,
            )
            alert_streaming(
                check="freshness",
                title="Data streaming tidak diperbarui",
                detail=petunjuk,
                observed=f"snapshot terbaru {row['latest']} ({age_minutes} menit lalu)",
                threshold=f"{FRESHNESS_THRESHOLD_MINUTES} menit",
                severity="CRITICAL",
            )
            return {
                "status": "STALE",
                "age_minutes": age_minutes,
                "latest": str(row["latest"]),
            }

        log.info(
            "Freshness OK: snapshot terbaru %s (%.1f menit lalu).",
            row["latest"], age_minutes,
        )
        return {
            "status": "OK",
            "age_minutes": age_minutes,
            "latest": str(row["latest"]),
            "total_snapshot": int(row["total_snapshot"]),
        }

    # --------------------------------------------------------
    # 2. Consumer liveness (datalake)
    # --------------------------------------------------------
    @task
    def check_consumer_liveness() -> dict:
        """Periksa apakah consumer masih menulis ke datalake.

        Dipisah dari pemeriksaan freshness supaya bisa membedakan dua
        kondisi yang gejalanya mirip: consumer berhenti menulis, atau
        consumer menulis tetapi load ke BigQuery yang gagal.
        """
        age = gcs_utils.newest_blob_age_seconds(f"{GCS_PREFIX_STATION_STATUS}/")

        if age is None:
            hasil = {
                "status": "EMPTY",
                "detail": f"Tidak ada objek di prefix {GCS_PREFIX_STATION_STATUS}/.",
            }
            log.error("Watchdog consumer: %s", hasil)
            alert_streaming(
                check="consumer_liveness",
                title="Consumer belum pernah menulis ke datalake",
                detail=hasil["detail"],
                observed="tidak ada objek",
                threshold=f"{CONSUMER_LIVENESS_MINUTES} menit",
                severity="CRITICAL",
            )
            return hasil

        age_minutes = round(age / 60, 1)
        if age > CONSUMER_LIVENESS_MINUTES * 60:
            log.error(
                "Consumer tidak menulis %.1f menit (ambang %d menit).",
                age_minutes, CONSUMER_LIVENESS_MINUTES,
            )
            alert_streaming(
                check="consumer_liveness",
                title="Consumer berhenti menulis ke datalake",
                detail=(
                    "Periksa container streaming-consumer: "
                    "`docker compose ps streaming-consumer` dan lognya."
                ),
                observed=f"objek terbaru {age_minutes} menit lalu",
                threshold=f"{CONSUMER_LIVENESS_MINUTES} menit",
                severity="CRITICAL",
            )
            return {"status": "STALE", "age_minutes": age_minutes}

        log.info("Consumer liveness OK: objek terbaru %.1f menit lalu.", age_minutes)
        return {"status": "OK", "age_minutes": age_minutes}

    # --------------------------------------------------------
    # 3. Lonjakan DLQ
    # --------------------------------------------------------
    @task
    def check_dlq_growth() -> dict:
        """Periksa apakah payload mulai rusak beruntun.

        Lonjakan di DLQ berarti producer menerima respons yang tidak sesuai
        skema berulang kali — biasanya karena API GBFS mengubah bentuknya.
        """
        sql = f"""
            SELECT COUNT(*) AS rows_dlq
            FROM `{TABLE_DLQ}`
            WHERE failed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {DLQ_WINDOW_MINUTES} MINUTE)
        """
        try:
            rows_dlq = int(bq_utils.query_scalar(sql) or 0)
        except Exception as exc:  # noqa: BLE001
            log.info("DLQ belum bisa dibaca (%s) — dilewati.", type(exc).__name__)
            return {"status": "SKIPPED", "detail": str(exc)[:200]}

        if rows_dlq > DLQ_ALERT_ROWS:
            log.error(
                "DLQ melonjak: %d baris dalam %d menit.", rows_dlq, DLQ_WINDOW_MINUTES
            )
            alert_streaming(
                check="dlq_growth",
                title="Banyak payload gagal di-parse",
                detail=(
                    "Kemungkinan skema respons GBFS berubah. Periksa contoh "
                    "payload di tabel station_status_dlq."
                ),
                observed=f"{rows_dlq} baris",
                threshold=f"{DLQ_ALERT_ROWS} baris / {DLQ_WINDOW_MINUTES} menit",
                severity="WARNING",
            )
            return {"status": "SURGE", "rows_dlq": rows_dlq}

        log.info("DLQ OK: %d baris dalam %d menit.", rows_dlq, DLQ_WINDOW_MINUTES)
        return {"status": "OK", "rows_dlq": rows_dlq}

    # --------------------------------------------------------
    # 4. Proporsi karantina di staging
    # --------------------------------------------------------
    @task
    def check_staging_rejection() -> dict:
        """Periksa proporsi baris streaming yang masuk karantina.

        Kasus terbesar dari data nyata adalah sentinel epoch pada
        `last_reported`. Nilai itu wajar dalam jumlah kecil, tetapi proporsi
        yang melonjak menandakan skema sumber berubah.
        """
        sql = f"""
            SELECT
                (SELECT COUNT(*) FROM `{TABLE_STAGING_STATION_STATUS}`)          AS valid_rows,
                (SELECT COUNT(*) FROM `{TABLE_STAGING_STATION_STATUS_REJECTED}`) AS rejected_rows
        """
        row = next(iter(bq_utils.client().query(sql).result()), None)
        if row is None:
            return {"status": "SKIPPED"}

        valid = int(row["valid_rows"] or 0)
        rejected = int(row["rejected_rows"] or 0)
        total = valid + rejected
        pct = round(rejected / total * 100, 4) if total else 0.0

        if total == 0:
            log.info("Staging streaming masih kosong — pemeriksaan dilewati.")
            return {"status": "SKIPPED", "detail": "belum ada data"}

        if pct > REJECTION_THRESHOLD_PCT:
            log.error("Karantina streaming %.2f%% (ambang %.2f%%).", pct, REJECTION_THRESHOLD_PCT)
            alert_streaming(
                check="staging_rejection",
                title="Proporsi karantina streaming melonjak",
                detail=(
                    "Periksa alasan penolakan di stg_station_status_rejected "
                    "untuk melihat pola yang muncul."
                ),
                observed=f"{pct}% ({rejected} dari {total} baris)",
                threshold=f"{REJECTION_THRESHOLD_PCT}%",
                severity="WARNING",
            )
            return {"status": "SURGE", "rejected_pct": pct}

        log.info("Karantina streaming OK: %.2f%% (%d dari %d).", pct, rejected, total)
        return {"status": "OK", "rejected_pct": pct, "rejected_rows": rejected}

    @task
    def summarize(hasil: list) -> dict:
        """Gabungkan hasil semua pemeriksaan menjadi satu ringkasan log.

        Menyediakan satu baris yang bisa dibaca cepat di UI Airflow, tanpa
        perlu membuka tiap task.
        """
        bersih = [h for h in hasil if isinstance(h, dict)]
        masalah = [h for h in bersih if h.get("status") not in ("OK", "SKIPPED")]
        ringkasan = {
            "checks": len(bersih),
            "issues": len(masalah),
            "status": [h.get("status") for h in bersih],
        }
        if masalah:
            log.warning("Watchdog menemukan %d masalah: %s", len(masalah), json.dumps(ringkasan))
        else:
            log.info("Watchdog: semua pemeriksaan sehat — %s", json.dumps(ringkasan))
        return ringkasan

    # Keempat pemeriksaan independen, jadi dijalankan paralel. Watchdog
    # tidak boleh memblokir apa pun, sehingga tidak ada wiring sekuensial.
    summarize([
        check_freshness(),
        check_consumer_liveness(),
        check_dlq_growth(),
        check_staging_rejection(),
    ])


citibike_watchdog_streaming()
