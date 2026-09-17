"""DAG: transformasi dbt (staging -> intermediate -> marts).

Tidak memakai jam. Terpicu Dataset ``RAW_TRIPS`` yang dipancarkan
``citibike_ingest_trips``, sehingga transformasi tidak pernah berjalan di atas
data yang belum siap.

    dbt deps                pasang paket (dbt_utils)
    dbt run tag:staging     stg_* + pemetaan id stasiun
    check_quarantine_surge  gate: hentikan bila karantina trip melonjak
    dbt run sisanya         dims, intermediate, marts core & dashboard
    dbt test                unique / not_null / relationships / expression
    dbt docs generate       artefak dokumentasi
    record_dq_metrics       catat metrik ke dq_metrics

Catatan pengembangan:

- Gate DQ diletakkan SEBELUM model lain dibangun agar data mencurigakan tidak
  pernah terbit ke dashboard. Gate ini hanya memeriksa sumber trip; karantina
  streaming diperiksa DAG ``citibike_transform_streaming``.

- Model setelah staging dijalankan dalam SATU perintah, bukan per tag. Grafik
  ketergantungan tidak mengikuti urutan layer: ``int_station_risk_calculation``
  (intermediate) membutuhkan ``dim_station`` (core). Menjalankan per tag gagal
  pada build dari nol karena dimensi belum ada.

- DAG ini WAJIB aktif. DAG berjadwal Dataset yang ter-pause tidak terpicu dan
  kegagalannya sunyi, tanpa error apa pun.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

from common import bq_utils
from common.alert_utils import (
    alert_data_quality,
    on_failure_alert,
)
from common.config import (
    TABLE_DQ_METRICS,
    TABLE_STAGING_TRIPS,
    TABLE_STAGING_TRIPS_REJECTED,
)
from common.datasets import RAW_TRIPS
from common.dbt_runner import dbt_task

log = logging.getLogger(__name__)

QUARANTINE_THRESHOLD_PCT = 5.0

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_failure_alert,
}


@dag(
    dag_id="citibike_transform_batch",
    description="dbt: staging -> (gate DQ) -> dims -> intermediate -> marts core & dashboard",
    schedule=[RAW_TRIPS],          # data-aware: menunggu ingestion selesai
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # PENTING: DAG berbasis Dataset harus DALAM keadaan aktif.
    # DAG yang ter-pause tidak akan terpicu oleh dataset event, dan
    # kegagalannya SUNYI — tidak ada error, ingestion tetap terlihat
    # sukses, tetapi transformasi tidak pernah berjalan.
    # Flag ini menimpa AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true.
    is_paused_upon_creation=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    # Sengaja TIDAK memakai on_failure_callback tingkat DAG; ringkasan
    # kegagalan per DAG-run ditangani citibike_watchdog_pipeline dengan
    # membaca metadata Airflow. Alasan lengkapnya ada di ingest_trips.py,
    # yang membuktikan callback tingkat DAG tidak andal. Alert per task
    # tetap aktif lewat default_args dan memuat tautan log untuk debug.
    default_args=default_args,
    tags=["citibike", "batch", "transform", "dbt"],
    doc_md=__doc__,
)
def citibike_transform_batch() -> None:

    # --------------------------------------------------------
    # Gate kualitas data
    # --------------------------------------------------------
    @task
    def check_quarantine_surge() -> dict:
        """Hentikan pipeline bila proporsi karantina trip melonjak.

        Dibaca langsung dari tabel staging (bukan dari model intermediate),
        supaya gate dapat berjalan tepat setelah staging selesai — sebelum
        apa pun naik ke layer berikutnya. Dengan begitu data bermasalah tidak
        pernah sempat "terbit" ke marts.

        **Hanya memeriksa sumber trip.** Karantina streaming diperiksa oleh
        DAG `citibike_transform_streaming` dengan ambangnya sendiri.

        Sebelumnya gate ini memeriksa trip DAN station_status, lalu
        menghentikan seluruh transformasi. Itu kopling yang salah arah:
        kualitas data streaming yang memburuk akan memblokir mart trip yang
        tidak ada hubungannya. Setiap aliran kini menjaga ambangnya sendiri.
        """
        sql = f"""
            WITH per_sumber AS (
                SELECT _source_file AS sumber, COUNT(*) AS rejected_rows
                FROM `{TABLE_STAGING_TRIPS_REJECTED}`
                GROUP BY _source_file
            ),
            total_per_sumber AS (
                SELECT _source_file AS sumber, COUNT(*) AS total_rows
                FROM `{TABLE_STAGING_TRIPS}`
                GROUP BY _source_file
            )
            SELECT
                -- Kolom `layer` dipertahankan agar pemanggilan
                -- alert_data_quality() di bawah tidak perlu diubah.
                'trips' AS layer,
                COALESCE(r.sumber, '(tidak diketahui)') AS sumber,
                t.total_rows,
                r.rejected_rows,
                ROUND(
                    SAFE_DIVIDE(r.rejected_rows, t.total_rows + r.rejected_rows) * 100,
                    4
                ) AS rejected_pct
            FROM per_sumber AS r
            LEFT JOIN total_per_sumber AS t
                ON r.sumber = t.sumber
            ORDER BY rejected_pct DESC
        """
        rows = [dict(r) for r in bq_utils.client().query(sql).result()]
        log.info("Ringkasan DQ per sumber: %s", json.dumps(rows, default=str))

        violations = [
            r for r in rows
            if (r["rejected_pct"] or 0) > QUARANTINE_THRESHOLD_PCT
        ]

        for v in violations:
            alert_data_quality(
                layer=v["layer"],
                dag_id="citibike_transform_batch",
                rejected_pct=float(v["rejected_pct"]),
                threshold_pct=QUARANTINE_THRESHOLD_PCT,
                total_rows=int(v["total_rows"] or 0),
                rejected_rows=int(v["rejected_rows"]),
            )

        totals = {
            "sumber": len(rows),
            "total_rows": sum(int(r["total_rows"] or 0) for r in rows),
            "rejected_rows": sum(int(r["rejected_rows"]) for r in rows),
            "violations": len(violations),
        }
        log.info("Gate DQ: %s", totals)

        if violations:
            raise ValueError(
                f"Lonjakan karantina melebihi {QUARANTINE_THRESHOLD_PCT}% pada "
                f"{len(violations)} sumber: "
                + ", ".join(
                    f"{v['layer']}/{v['sumber']}={v['rejected_pct']}%" for v in violations
                )
            )
        return totals

    # --------------------------------------------------------
    # Task dbt (semua dibuat lewat wrapper dbt_runner)
    # --------------------------------------------------------
    deps = dbt_task("dbt_deps", "deps", retries=2)
    run_staging = dbt_task("dbt_run_staging", "run", "tag:staging")

    dq_gate = check_quarantine_surge()

    # Model sisanya dijalankan dalam SATU perintah agar dbt sendiri yang
    # mengurutkan berdasarkan ketergantungan. Urutan tag tidak dipakai di
    # sini karena grafiknya tidak mengikuti urutan layer secara ketat:
    # int_station_risk_calculation (intermediate) membutuhkan dim_station
    # (core) untuk memperoleh kapasitas stasiun. Menjalankan per tag akan
    # gagal pada build dari nol karena dimensi belum ada saat layer
    # intermediate dieksekusi.
    run_rest = dbt_task("dbt_run_remaining", "run", extra="--exclude tag:staging")
    run_all_tests = dbt_task("dbt_test", "test", retries=0)
    gen_docs = dbt_task("dbt_docs_generate", "docs generate", retries=0)

    # --------------------------------------------------------
    # Catat metrik kualitas data
    # --------------------------------------------------------
    @task
    def record_dq_metrics(**context) -> dict:
        """Simpan metrik DQ batch & streaming ke tabel `dq_metrics`.

        Tabel ini adalah riwayat kualitas data: berapa baris valid dan berapa
        dikarantina pada setiap pemrosesan, untuk kedua sumber. Tanpanya,
        pertanyaan seperti "apakah kualitas data membaik atau memburuk?"
        hanya bisa dijawab dengan membuka log satu per satu.

        Ditulis setelah `dbt test` lulus, sehingga angka yang tercatat
        berasal dari model yang sudah terverifikasi.
        """
        run_id = context.get("run_id", "unknown")
        sql = f"""
            WITH streaming AS (
                SELECT
                    (SELECT COUNT(*) FROM `{TABLE_STAGING_STATION_STATUS}`)          AS valid_rows,
                    (SELECT COUNT(*) FROM `{TABLE_STAGING_STATION_STATUS_REJECTED}`) AS rejected_rows
            ),
            batch AS (
                SELECT
                    (SELECT COUNT(*) FROM `{TABLE_STAGING_TRIPS}`)          AS valid_rows,
                    (SELECT COUNT(*) FROM `{TABLE_STAGING_TRIPS_REJECTED}`) AS rejected_rows
            )
            SELECT 'station_status' AS layer, s.* FROM streaming AS s
            UNION ALL
            SELECT 'trips' AS layer, b.* FROM batch AS b
        """
        rows = [dict(r) for r in bq_utils.client().query(sql).result()]

        payload = []
        for r in rows:
            valid = int(r["valid_rows"] or 0)
            rejected = int(r["rejected_rows"] or 0)
            total = valid + rejected
            payload.append(
                {
                    "run_date": None,  # diisi BigQuery di bawah
                    "layer": r["layer"],
                    "run_id": run_id,
                    "total_rows": total,
                    "valid_rows": valid,
                    "rejected_rows": rejected,
                    "duplicate_rows": 0,
                    "rejected_pct": round(rejected / total * 100, 4) if total else 0.0,
                }
            )

        if payload:
            # `noted_at` diisi eksplisit: tanpa itu kolomnya tetap NULL, dan
            # metrik kualitas data yang tidak punya cap waktu pencatatan
            # tidak bisa dipakai untuk melihat tren.
            noted_at = datetime.now(timezone.utc).isoformat()
            errors = bq_utils.client().insert_rows_json(
                TABLE_DQ_METRICS,
                [
                    {
                        **p,
                        "run_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        "noted_at": noted_at,
                    }
                    for p in payload
                ],
            )
            if errors:
                # Best-effort: metrik bukan data analitis, jadi kegagalan
                # mencatatnya tidak boleh menggagalkan pipeline.
                log.warning("Sebagian metrik DQ gagal disimpan: %s", errors[:2])

        ringkasan = {
            p["layer"]: {"total": p["total_rows"], "rejected": p["rejected_rows"],
                         "rejected_pct": p["rejected_pct"]}
            for p in payload
        }
        log.info("Metrik DQ tercatat: %s", json.dumps(ringkasan))
        return ringkasan

    # --------------------------------------------------------
    # Wiring — gate DQ sebelum apa pun naik dari staging
    # --------------------------------------------------------
    deps >> run_staging >> dq_gate
    dq_gate >> run_rest >> run_all_tests >> gen_docs >> record_dq_metrics()


citibike_transform_batch()
