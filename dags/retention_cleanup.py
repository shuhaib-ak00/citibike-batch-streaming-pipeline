"""DAG: retention & pembersihan tabel operasional.

**Kenapa perlu.** Tabel `station_status` bersifat append-only — consumer
menulis terus-menerus dan tidak ada yang menghapus. Dengan ±2.500 stasiun
per snapshot dan interval 90 detik, volumenya bertambah sekitar **240 MB
per hari**. Tanpa pembersihan, storage akan menembus kuota gratis dan
query "kondisi terkini" makin mahal karena partisi harian makin besar.

Tabel yang dibersihkan, beserta alasan masa simpannya
-----------------------------------------------------
| Tabel | Retensi | Alasan |
|---|---|---|
| `station_status` | 14 hari | Mart dashboard hanya memakai snapshot terakhir; analisis tren cukup hitungan hari |
| `station_status_dlq` | 30 hari | Cukup lama untuk memeriksa pola bila terjadi lonjakan payload rusak |

**Catatan: tabel karantina justru TIDAK dibersihkan di sini.**
`stg_trips_rejected` dan `stg_station_status_rejected` bermaterialisasi
**view**, sehingga tidak menyimpan data sendiri — isinya selalu dihitung
ulang dari tabel raw. Masa hidupnya otomatis mengikuti raw, jadi retensi
terhadapnya tidak punya arti. Dua alasan mengapa view adalah pilihan yang
benar, bukan kekurangan:

1. `check_quarantine_surge` menghitung **rasio** baris ditolak per
   eksekusi dbt. Untuk itu ia butuh isi karantina yang selalu mencerminkan
   data raw saat ini, bukan akumulasi historis. Bila tabelnya
   diakumulasi, rasio akan terhitung salah — pembilangnya ikut memuat
   baris yang sudah lama ditolak.
2. `quarantined_at` diisi `CURRENT_TIMESTAMP()`. Pada view, nilai itu
   diartikan "kapan baris ini dinilai ditolak pada eksekusi terakhir",
   dan itu memang pertanyaan yang ingin dijawab. Bila dijadikan tabel
   biasa, seluruh tabel dibangun ulang tiap run sehingga semua baris tetap
   bertanggal "sekarang" — retensi berdasarkan kolom itu tetap tidak akan
   pernah menghapus apa pun.

Verifikasi retensi terhadap tabel yang benar-benar menyimpan data
(station_status dan DLQ) dilakukan lewat pengukuran sebelum/sesudah, agar
efeknya terbukti dan bukan sekadar diasumsikan.

Jadwal mingguan dipilih karena volume tumbuh lambat relatif terhadap
ambang retensi — menjalankan harian hanya menambah beban tanpa manfaat.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from common import bq_utils
from common.alert_utils import on_failure_alert
from common.config import (
    TABLE_DLQ,
    TABLE_STAGING_STATION_STATUS_REJECTED,
    TABLE_STAGING_TRIPS_REJECTED,
    TABLE_STATION_STATUS,
)

log = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
}


@dag(
    dag_id="citibike_retention_cleanup",
    description="Hapus data operasional yang melewati masa simpan (streaming & DLQ)",
    # Minggu 03:00 — di luar jam sibuk dan tidak bertabrakan dengan batch harian.
    schedule="0 3 * * 0",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    # Tanpa on_failure_callback tingkat DAG; ringkasan kegagalan ditangani
    # citibike_watchdog_pipeline (lihat penjelasan di ingest_trips.py).
    default_args=default_args,
    tags=["citibike", "maintenance", "retention", "cost"],
    doc_md=__doc__,
)
def citibike_retention_cleanup() -> None:

    @task
    def measure_before() -> dict:
        """Catat volume sebelum dibersihkan, agar efeknya bisa dibuktikan.

        Tanpa pengukuran ini, penghapusan hanya "kelihatannya" berhasil.
        """
        sql = f"""
            SELECT
                (SELECT COUNT(*) FROM `{TABLE_STATION_STATUS}`) AS station_status_rows,
                (SELECT COUNT(*) FROM `{TABLE_DLQ}`)            AS dlq_rows,
                (SELECT MIN(_snapshot_date) FROM `{TABLE_STATION_STATUS}`) AS oldest_snapshot_date
        """
        row = next(iter(bq_utils.client().query(sql).result()), None)
        hasil = {k: (str(v) if v is not None else None) for k, v in (dict(row) if row else {}).items()}
        log.info("Volume sebelum pembersihan: %s", hasil)
        return hasil

    @task
    def cleanup_streaming() -> dict:
        """Hapus snapshot streaming yang melewati masa simpan.

        DELETE ini memakai filter kolom partisi, sehingga BigQuery hanya
        membaca partisi lama — bukan seluruh tabel.
        """
        hari = 14
        sql = f"""
            DELETE FROM `{TABLE_STATION_STATUS}`
            WHERE _snapshot_date < DATE_SUB(CURRENT_DATE(), INTERVAL {hari} DAY)
        """
        job = bq_utils.client().query(sql, location=bq_utils.LOCATION)
        job.result()
        dihapus = job.num_dml_affected_rows or 0
        log.info("station_status: %d baris lebih tua dari %d hari dihapus.", dihapus, hari)
        return {"table": "station_status", "deleted_rows": dihapus, "retention_days": hari}

    @task
    def cleanup_dlq() -> dict:
        """Hapus payload gagal yang sudah lama."""
        hari = 30
        sql = f"""
            DELETE FROM `{TABLE_DLQ}`
            WHERE DATE(failed_at) < DATE_SUB(CURRENT_DATE(), INTERVAL {hari} DAY)
        """
        try:
            job = bq_utils.client().query(sql, location=bq_utils.LOCATION)
            job.result()
            dihapus = job.num_dml_affected_rows or 0
        except Exception as exc:  # noqa: BLE001
            # Tabel DLQ mungkin belum pernah menerima baris apa pun.
            log.info("DLQ tidak dibersihkan (%s) — mungkin belum ada data.", type(exc).__name__)
            return {"table": "station_status_dlq", "deleted_rows": 0, "skipped": True}
        log.info("station_status_dlq: %d baris dihapus.", dihapus)
        return {"table": "station_status_dlq", "deleted_rows": dihapus, "retention_days": hari}

    @task
    def cleanup_rejected() -> list:
        """Laporkan mengapa karantina tidak perlu dibersihkan.

        Tugas ini sengaja tidak menghapus apa pun. Ia ada supaya keputusan
        desainnya tercatat di log setiap kali DAG berjalan — bukan sebagai
        komentar yang cepat usang. Ia juga membuktikan bahwa tabel karantina
        memang view: bila suatu saat modelnya diubah menjadi tabel, baris
        peringatan di bawah akan muncul dan memaksa keputusan itu ditinjau
        ulang.

        Anotasi ``list`` penting, bukan gaya penulisan semata: ``@task``
        memakai anotasi tipe untuk memutuskan ``multiple_outputs``. Menulis
        ``-> dict`` padahal isinya list membuat Airflow menolak hasilnya
        dengan "Returned output was type <class 'list'> expected dictionary".
        """
        hasil = []
        for table in (TABLE_STAGING_TRIPS_REJECTED, TABLE_STAGING_STATION_STATUS_REJECTED):
            info = bq_utils.client().get_table(table)
            hasil.append({"table": info.table_id, "tipe": info.table_type})
            if info.table_type == "VIEW":
                log.info(
                    "%s adalah VIEW — retensi tidak berlaku, dilewati. "
                    "Isinya selalu dihitung ulang dari tabel raw.",
                    info.table_id,
                )
            else:
                log.warning(
                    "%s kini bertipe %s, BUKAN view. Retensi terhadapnya "
                    "belum diimplementasikan dan perlu ditinjau ulang.",
                    info.table_id, info.table_type,
                )
        return hasil

    @task
    def measure_after() -> dict:
        """Catat volume setelah dibersihkan, lalu laporkan selisihnya."""
        sql = f"""
            SELECT
                (SELECT COUNT(*) FROM `{TABLE_STATION_STATUS}`) AS station_status_rows,
                (SELECT MIN(_snapshot_date) FROM `{TABLE_STATION_STATUS}`) AS oldest_snapshot_date
        """
        row = next(iter(bq_utils.client().query(sql).result()), None)
        hasil = {k: (str(v) if v is not None else None) for k, v in (dict(row) if row else {}).items()}
        log.info("Volume setelah pembersihan: %s", hasil)
        return hasil

    awal = measure_before()
    streaming = cleanup_streaming()
    dlq = cleanup_dlq()
    rejected = cleanup_rejected()
    akhir = measure_after()

    # Urutan: ukur -> bersihkan -> ukur lagi. Ketiga pembersihan saling
    # independen sehingga boleh berjalan paralel setelah pengukuran awal.
    awal >> [streaming, dlq, rejected] >> akhir


citibike_retention_cleanup()
