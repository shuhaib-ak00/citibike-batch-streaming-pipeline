"""DAG: transformasi streaming (dbt) — refresh rantai mart operasional.

**Masalah yang diselesaikan.** Sebelumnya seluruh model dbt hanya dibangun
saat `citibike_transform_batch` berjalan, dan DAG itu terpicu sekali sehari
oleh Dataset `RAW_TRIPS`. Akibatnya mart streaming — yang sumbernya
bertambah tiap ±90 detik — menampilkan kondisi **hampir 24 jam lalu**.
Auto-refresh Metabase hanya membaca ulang isi tabel yang sama, jadi peta
"kondisi terkini" tidak pernah benar-benar terkini.

DAG ini memperbarui **hanya rantai streaming**, setiap jam.

Mengapa daftar model eksplisit
------------------------------
1. **Tanpa tanda `+`.** `dbt run --select +station_availability_realtime`
   ikut membangun seluruh leluhurnya, termasuk `dim_station` -> `stg_trips`
   -> memindai `raw.trips` (1,1 GiB) setiap jam. Dimensi bersama cukup
   dipakai dari hasil run harian; kapasitas stasiun jarang berubah.
2. **Tanpa tag.** `int_station_risk_calculation` (streaming) membutuhkan
   `dim_station` (batch), jadi tag murni tidak cukup dan urutannya tidak
   dapat diandalkan.

Daftar ini bersifat **optimasi**, bukan satu-satunya penjamin: DAG batch
menjalankan `--exclude tag:staging` sehingga tetap membangun model apa pun
yang belum tercantum di sini. Model baru karena itu tidak akan "hilang",
hanya tertunda sampai run harian berikutnya.

Mengapa gate DQ-nya terpisah
----------------------------
Gate di DAG batch dulu memeriksa trip DAN station_status, lalu menghentikan
SELURUH transformasi. Artinya kualitas data streaming yang memburuk akan
memblokir mart trip yang tidak ada hubungannya. Setiap aliran kini menjaga
ambangnya sendiri.

Mengapa `target_path` terpisah
------------------------------
Kedua DAG kini dapat menjalankan dbt bersamaan. Tanpa direktori target
terpisah, `dbt test` dapat membaca `manifest.json` yang sedang ditulis
proses lain. Artefak itu hanya metadata kompilasi — data di warehouse tetap
aman karena `CREATE OR REPLACE` bersifat atomik.

Mengapa `dbt deps` tetap dijalankan di sini
-------------------------------------------
Paket dbt dipasang ke `/tmp` di dalam container, bukan ke `dbt_packages/` di
dalam bind-mount `./dbt` — lihat `DBT_PACKAGES_INSTALL_PATH` di
docker-compose.yml dan `packages-install-path` di dbt_project.yml.

Karena `/tmp` bersifat sementara (hilang saat container di-recreate), DAG ini
memasang paketnya sendiri agar tidak bergantung pada apakah DAG batch sudah
berjalan hari itu. Sebelumnya paket disimpan di dalam bind-mount, dan di sana
berkasnya hilang berulang kali sehingga `dbt deps` justru berakhir dengan
paket yang rusak.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from common import bq_utils
from common.alert_utils import alert_data_quality, on_failure_alert
from common.config import (
    TABLE_STAGING_STATION_STATUS,
    TABLE_STAGING_STATION_STATUS_REJECTED,
)
from common.dbt_runner import dbt_task

log = logging.getLogger(__name__)

QUARANTINE_THRESHOLD_PCT = 5.0

# Direktori artefak dbt khusus DAG ini; lihat catatan di docstring modul.
TARGET_PATH = "/tmp/dbt-target-streaming"

# Staging dipisah dari sisanya supaya gate DQ dapat berjalan di antaranya —
# sebelum lapisan mana pun di atasnya dibangun.
STAGING_MODELS = "stg_station_status stg_station_status_rejected"

DOWNSTREAM_MODELS = " ".join(
    [
        "int_station_risk_calculation",
        "int_latest_complete_snapshot",
        "fct_station_status",
        # Snapshot-diff CDC: daftar perubahan + validity window-nya.
        # Diletakkan setelah fct_station_status karena keduanya membacanya,
        # dan dbt akan mengurutkan sendiri berdasarkan ketergantungan.
        "int_station_status_changes",
        "int_station_status_windows",
        "station_availability_realtime",
        "station_risk_monitoring",
    ]
)

ALL_STREAMING_MODELS = f"{STAGING_MODELS} {DOWNSTREAM_MODELS}"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_failure_alert,
}


@dag(
    dag_id="citibike_transform_streaming",
    description="dbt: refresh jam-an rantai streaming (status stasiun -> mart operasional)",
    # Tiap jam dinilai titik seimbang: cukup segar untuk dashboard
    # operasional, tanpa menambah beban query yang tidak sepadan manfaatnya.
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # Tanpa flag ini, DAG baru ter-pause pada clone segar dan kegagalannya
    # SUNYI — tidak ada error, hanya mart yang diam-diam tidak diperbarui.
    is_paused_upon_creation=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    default_args=default_args,
    tags=["citibike", "streaming", "transform", "dbt"],
    doc_md=__doc__,
)
def citibike_transform_streaming() -> None:

    @task
    def check_quarantine_surge() -> dict:
        """Hentikan refresh streaming bila proporsi karantina melonjak.

        Hanya memeriksa sumber `station_status`; sumber trip diperiksa DAG
        batch. Dibaca langsung dari tabel staging sehingga gate berjalan
        sebelum lapisan di atasnya dibangun.
        """
        sql = f"""
            SELECT
                t.total_rows,
                r.rejected_rows,
                ROUND(
                    SAFE_DIVIDE(r.rejected_rows, t.total_rows + r.rejected_rows) * 100,
                    4
                ) AS rejected_pct
            FROM (
                SELECT COUNT(*) AS rejected_rows
                FROM `{TABLE_STAGING_STATION_STATUS_REJECTED}`
            ) AS r
            CROSS JOIN (
                SELECT COUNT(*) AS total_rows
                FROM `{TABLE_STAGING_STATION_STATUS}`
            ) AS t
        """
        row = next(iter(bq_utils.client().query(sql).result()), None)
        hasil = dict(row) if row else {}
        total = int(hasil.get("total_rows") or 0)
        rejected = int(hasil.get("rejected_rows") or 0)
        pct = float(hasil.get("rejected_pct") or 0.0)

        log.info(
            "Gate DQ streaming: %d valid + %d karantina = %.4f%% (ambang %.2f%%)",
            total, rejected, pct, QUARANTINE_THRESHOLD_PCT,
        )

        if total + rejected == 0:
            # Belum ada data sama sekali. Bukan masalah kualitas data, dan
            # bukan alasan menghentikan pipeline — model akan dibangun kosong.
            log.info("Staging streaming masih kosong — gate dilewati.")
            return {"sumber": "station_status", "total_rows": 0,
                    "rejected_rows": 0, "rejected_pct": 0.0, "violations": 0}

        if pct > QUARANTINE_THRESHOLD_PCT:
            alert_data_quality(
                layer="station_status",
                dag_id="citibike_transform_streaming",
                rejected_pct=pct,
                threshold_pct=QUARANTINE_THRESHOLD_PCT,
                total_rows=total,
                rejected_rows=rejected,
            )
            raise ValueError(
                f"Lonjakan karantina streaming {pct}% melebihi ambang "
                f"{QUARANTINE_THRESHOLD_PCT}% "
                f"({rejected} dari {total + rejected} baris)."
            )

        return {
            "sumber": "station_status",
            "total_rows": total,
            "rejected_rows": rejected,
            "rejected_pct": pct,
            "violations": 0,
        }

    # Paket dipasang ke /tmp di dalam container (lihat DBT_PACKAGES_INSTALL_PATH
    # di docker-compose.yml). Karena /tmp sementara, task ini yang memastikan
    # paket tersedia — lihat catatan di docstring modul.
    deps = dbt_task("dbt_deps", "deps", retries=2, target_path=TARGET_PATH)

    run_staging = dbt_task(
        "dbt_run_staging_streaming",
        "run",
        STAGING_MODELS,
        target_path=TARGET_PATH,
    )

    dq_gate = check_quarantine_surge()

    run_marts = dbt_task(
        "dbt_run_marts_streaming",
        "run",
        DOWNSTREAM_MODELS,
        target_path=TARGET_PATH,
    )

    run_tests = dbt_task(
        "dbt_test_streaming",
        "test",
        ALL_STREAMING_MODELS,
        retries=0,
        target_path=TARGET_PATH,
    )

    # Sengaja TIDAK ada, beserta alasannya:
    #   dbt docs generate  -> artefak dokumentasi statis; sekali sehari dari
    #                         DAG batch sudah cukup.
    #   record_dq_metrics  -> tabel itu riwayat harian; baris per jam akan
    #                         menggelembungkannya 24x. Kualitas streaming
    #                         sudah dipantau citibike_watchdog_streaming
    #                         tiap 5 menit.
    deps >> run_staging >> dq_gate >> run_marts >> run_tests


citibike_transform_streaming()
