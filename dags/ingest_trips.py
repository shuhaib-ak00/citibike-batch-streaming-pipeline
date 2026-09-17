"""DAG: ingest Citi Bike trip history (batch).

Alur::

    CSV bulanan  ->  di-split per hari  ->  Parquet di GCS
                 ->  load per partisi harian ke BigQuery raw.trips

Tahapan task::

    discover_source_files -> split_and_upload (1 task per file)
      -> collect_partitions -> load_partition (1 task per tanggal)
      -> verify_row_counts

Karakteristik desain
--------------------
1. **File bulanan dipecah jadi partisi harian di datalake.** Kunci partisi
   adalah ``DATE(started_at)``, dipilih dari hasil profiling data awal.

2. **Satu file bisa menyumbang banyak tanggal, dan satu tanggal bisa
   menerima dari beberapa file.** Sumber Citi Bike dipotong per bulan
   memakai *ended date*, sehingga trip yang mulai 28 Feb tapi selesai 1 Mar
   berada di file Maret. Karena itu ``split_and_upload`` menulis Parquet per
   tanggal, dan ``load_partition`` menggabungkan **seluruh** file pada
   tanggal itu sebelum memuat — bukan per file, yang akan menghasilkan
   partisi terpecah dan hitungan tidak konsisten.

3. **Idempoten (re-run aman).** ``load_partition`` menjalankan
   *delete-partition lalu append* (lihat ``common/bq_utils.py``): baris pada
   tanggal itu dihapus dulu, lalu data dari GCS dimuat ulang. Hasil akhir
   selalu sama dengan isi datalake.

   Catatan: **bukan** memakai partition decorator (``trips$YYYYMMDD``).
   Decorator hanya berlaku untuk tabel ber-*ingestion-time partitioning*,
   sedangkan tabel ini dipartisi berdasarkan **kolom** ``_ride_started_date``;
   BigQuery menolaknya dengan "Table ... cannot include decorator".

4. **Format Parquet** dipakai agar tipe kolom terjaga saat berpindah dari
   datalake ke warehouse (``started_at`` tetap TIMESTAMP, koordinat tetap
   FLOAT64) dan ukuran berkas lebih kecil dibanding CSV.

5. **Pemisahan datalake dan warehouse tetap terjaga.** Data mentah disimpan
   di GCS sebagai Parquet; BigQuery hanya menyimpan salinan siap-query.
   Re-run tidak meng-upload ulang ke GCS karena blok yang sudah ada dipakai.

6. **Alert kegagalan pipeline** lewat ``on_failure_callback``, dan
   **verifikasi integritas** membandingkan isi datalake (GCS) dengan data
   warehouse (BigQuery) per partisi — kalau meleset, pipeline gagal sehingga
   alert terpicu. Jumlah baris datalake dibaca dari custom metadata objek
   Parquet, jadi tidak ada berkas besar yang diunduh saat verifikasi.

7. **Memicu DAG transformasi secara otomatis.** ``verify_row_counts``
   memancarkan Dataset ``RAW_TRIPS``, sehingga ``citibike_transform_batch``
   berjalan begitu data raw benar-benar siap (data-aware scheduling),
   bukan berdasarkan jam yang ditebak.
"""
from __future__ import annotations

import logging
import shutil
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.decorators import dag, task
from airflow.models.param import Param

from common import bq_utils, gcs_utils
from common.alert_utils import on_failure_alert
from common.config import (
    GCS_PREFIX_TRIPS,
    RAW_DIR,
    TABLE_TRIPS,
    TRIP_FILE_GLOB,
)
from common.datasets import RAW_TRIPS

log = logging.getLogger(__name__)

# ============================================================
# Skema Parquet — HARUS sinkron dengan:
#   - dags/common/bq_utils.py TRIPS_SCHEMA
#   - sql/bigquery/01_raw_tables_ddl.sql
# ============================================================
PARQUET_SCHEMA = pa.schema(
    [
        pa.field("ride_id", pa.string()),
        pa.field("rideable_type", pa.string()),
        pa.field("started_at", pa.timestamp("us")),
        pa.field("ended_at", pa.timestamp("us")),
        pa.field("start_station_name", pa.string()),
        pa.field("start_station_id", pa.string()),
        pa.field("end_station_name", pa.string()),
        pa.field("end_station_id", pa.string()),
        pa.field("start_lat", pa.float64()),
        pa.field("start_lng", pa.float64()),
        pa.field("end_lat", pa.float64()),
        pa.field("end_lng", pa.float64()),
        pa.field("member_casual", pa.string()),
        pa.field("_source_file", pa.string()),
        pa.field("_ingested_at", pa.timestamp("us")),
        pa.field("_ride_started_date", pa.date32()),
    ]
)

CHUNK_ROWS = 400_000

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_failure_alert,
}


@dag(
    dag_id="citibike_ingest_trips",
    description="Batch: CSV bulanan -> partisi harian di GCS -> BigQuery raw.trips",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,               # backfill dilakukan manual lewat Trigger DAG w/ conf
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    # Sengaja TIDAK memakai on_failure_callback tingkat DAG.
    #
    # Airflow membangun konteks callback DAG dari SATU task instance
    # sembarang (ti = tis[-1] di DAG.fetch_callback). DAG ini memakai
    # dynamic task mapping (split_and_upload.expand, load_partition.expand),
    # dan membangun konteks task hasil mapping yang gagal melempar
    # NotFullyPopulated — sehingga callback tidak pernah terkirim, justru
    # pada DAG yang paling rawan gagal sistemik.
    #
    # Ringkasan kegagalan per DAG-run ditangani citibike_watchdog_pipeline
    # dengan membaca metadata Airflow, bukan lewat callback. Alert per task
    # tetap aktif lewat default_args (punya tautan log untuk debug).
    default_args=default_args,
    tags=["citibike", "batch", "ingestion", "raw"],
    doc_md=__doc__,
    params={
        "file_pattern": Param(
            TRIP_FILE_GLOB,
            type="string",
            description="Glob file sumber di folder data/raw (di dalam container).",
        ),
        "limit_files": Param(
            0,
            type="integer",
            minimum=0,
            description="0 = semua file. >0 = batasi jumlah file (untuk uji cepat).",
        ),
    },
)
def citibike_ingest_trips() -> None:

    # --------------------------------------------------------
    # 1. Temukan file sumber
    # --------------------------------------------------------
    @task
    def discover_source_files(**context) -> list[str]:
        params = context["params"]
        pattern = params["file_pattern"]
        limit = int(params["limit_files"] or 0)

        files = sorted(p.name for p in RAW_DIR.glob(pattern))
        if not files:
            raise FileNotFoundError(
                f"Tidak ada file cocok '{pattern}' di {RAW_DIR}. "
                f"Cek bind mount ./data/raw di docker-compose."
            )

        if limit > 0:
            files = files[:limit]

        log.info("Ditemukan %d file sumber: %s", len(files), files)
        return files

    # --------------------------------------------------------
    # 2. Split per hari + upload Parquet ke GCS
    #    (satu task instance per file -> terlihat paralel di UI Airflow)
    # --------------------------------------------------------
    @task(max_active_tis_per_dag=2)
    def split_and_upload(fname: str, **context) -> dict:
        """Baca 1 file CSV, pecah per tanggal, tulis Parquet ke GCS.

        Direktori sementara selalu dibersihkan di blok ``finally``. Tanpa itu,
        setiap run meninggalkan Parquet sisa di /tmp container (~150 MB per
        file) dan lambat laun memenuhi disk.
        """
        src = RAW_DIR / fname
        ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        per_date: dict[str, int] = {}
        buffer = Path(tempfile.mkdtemp(prefix="citibike_"))
        log.info("Memproses %s (%d bytes)", fname, src.stat().st_size)

        try:
            reader = pd.read_csv(src, dtype=str, chunksize=CHUNK_ROWS)
            for chunk_no, chunk in enumerate(reader, start=1):
                # ---- normalisasi tipe (semua dibaca sebagai string) ----
                chunk = chunk.assign(
                    started_at=pd.to_datetime(chunk["started_at"], errors="coerce"),
                    ended_at=pd.to_datetime(chunk["ended_at"], errors="coerce"),
                    start_lat=pd.to_numeric(chunk["start_lat"], errors="coerce"),
                    start_lng=pd.to_numeric(chunk["start_lng"], errors="coerce"),
                    end_lat=pd.to_numeric(chunk["end_lat"], errors="coerce"),
                    end_lng=pd.to_numeric(chunk["end_lng"], errors="coerce"),
                )
                # Baris tanpa started_at tidak bisa ditempatkan di partisi mana
                # pun. Tidak dibuang diam-diam: jumlahnya dicatat di log.
                n_missing_ts = int(chunk["started_at"].isna().sum())
                if n_missing_ts:
                    log.warning("%s chunk %d: %d baris tanpa started_at dilewati.",
                                fname, chunk_no, n_missing_ts)
                chunk = chunk[chunk["started_at"].notna()].copy()

                chunk["_source_file"] = fname
                chunk["_ingested_at"] = ingested_at
                chunk["_ride_started_date"] = chunk["started_at"].dt.date

                # ---- tulis satu Parquet per tanggal ----
                # Nama berkas memuat nomor chunk karena satu file sumber besar
                # dibaca bertahap agar tidak kehabisan memori.
                for ride_date, group in chunk.groupby("_ride_started_date", sort=True):
                    out_dir = buffer / ride_date.isoformat()
                    out_dir.mkdir(parents=True, exist_ok=True)
                    out_file = out_dir / f"{Path(fname).stem}-chunk{chunk_no:03d}.parquet"

                    group = group[[f.name for f in PARQUET_SCHEMA]]
                    table = pa.Table.from_pandas(
                        group, schema=PARQUET_SCHEMA, preserve_index=False
                    )
                    pq.write_table(table, out_file, compression="snappy")

                    blob = (
                        f"{GCS_PREFIX_TRIPS}/dt={ride_date.isoformat()}/{out_file.name}"
                    )
                    # Jumlah baris dicatat sebagai custom metadata objek agar
                    # verifikasi bisa menghitung isi datalake tanpa mengunduh
                    # berkas Parquet-nya.
                    gcs_utils.upload_file(
                        out_file,
                        blob,
                        metadata={
                            gcs_utils.ROW_COUNT_METADATA_KEY: len(group),
                            "source_file": fname,
                        },
                    )
                    per_date[ride_date.isoformat()] = (
                        per_date.get(ride_date.isoformat(), 0) + len(group)
                    )
        finally:
            # Selalu bersihkan, baik run sukses maupun gagal, agar tidak
            # menumpuk berkas Parquet sementara di disk container.
            shutil.rmtree(buffer, ignore_errors=True)

        total = sum(per_date.values())
        log.info("%s -> %d baris, %d partisi tanggal", fname, total, len(per_date))
        return {"file": fname, "total_rows": total, "per_date": per_date}

    # --------------------------------------------------------
    # 3. Kumpulkan daftar partisi harian
    # --------------------------------------------------------
    @task
    def collect_partitions(_split_results: list, **context) -> list[str]:
        """Ambil daftar tanggal dari prefix GCS (bukan dari XCom) agar idempoten.

        Karena partisi dibaca dari datalake, DAG tetap benar walau hanya sebagian
        file diproses pada suatu run (mis. saat ``limit_files`` dipakai untuk uji
        cepat): partisi yang sudah ada dari run sebelumnya ikut diproses ulang.
        Argumen ``_split_results`` hanya untuk memaksa dependensi pada task split.
        """
        prefixes = gcs_utils.list_prefixes(f"{GCS_PREFIX_TRIPS}/")
        dates = []
        for p in prefixes:
            # p berbentuk 'raw/trips/dt=2026-01-02/'
            part = p.rstrip("/").rsplit("dt=", 1)[-1]
            try:
                date.fromisoformat(part)
                dates.append(part)
            except ValueError:
                log.warning("Prefix tidak dikenali sebagai tanggal: %s", p)
        log.info("Ditemukan %d partisi harian di GCS", len(dates))
        return sorted(dates)

    # --------------------------------------------------------
    # 4. Load tiap partisi ke BigQuery (idempoten per partisi)
    # --------------------------------------------------------
    @task(max_active_tis_per_dag=4, retries=3, retry_delay=timedelta(minutes=1))
    def load_partition(partition: str) -> dict:
        part_date = date.fromisoformat(partition)
        blobs = gcs_utils.list_blobs(f"{GCS_PREFIX_TRIPS}/dt={partition}/")
        if not blobs:
            log.warning("Partisi %s tidak punya file — dilewati.", partition)
            return {"date": partition, "rows": 0, "files": 0}

        uris = [f"gs://{gcs_utils.BUCKET_RAW}/{b}" for b in blobs]
        rows = bq_utils.load_parquet_partition(
            table_id=TABLE_TRIPS,
            partition_date=part_date,
            source_uris=uris,
            schema=bq_utils.TRIPS_SCHEMA,
        )
        return {"date": partition, "rows": rows, "files": len(uris)}

    # --------------------------------------------------------
    # 5. Verifikasi: jumlah baris GCS vs BigQuery per partisi
    # --------------------------------------------------------
    @task(outlets=[RAW_TRIPS])
    def verify_row_counts(**context) -> dict:
        """Bandingkan isi datalake (GCS) dengan data warehouse (BigQuery).

        Sumber pembanding adalah **isi GCS sebenarnya**, bukan hasil split run
        ini. Perbedaan ini penting: ``load_partition`` memuat SELURUH berkas pada
        satu tanggal (karena datalake adalah sumber kebenaran), sehingga bila
        hanya sebagian berkas yang di-upload pada suatu run — mis. saat memakai
        ``limit_files`` — membandingkan dengan hasil split run ini akan
        menghasilkan selisih palsu.

        Jumlah baris per tanggal dihitung dari custom metadata objek Parquet,
        jadi tidak ada berkas besar yang perlu diunduh.

        Bila tidak cocok, task GAGAL sehingga alert kegagalan pipeline terpicu.
        Task ini juga memancarkan Dataset ``RAW_TRIPS`` sebagai pemicu DAG
        transformasi dbt.
        """
        partitions = context["ti"].xcom_pull(task_ids="collect_partitions") or []

        mismatches: list[dict] = []
        total_expected = 0
        total_actual = 0
        fallback_objects = 0

        for part in sorted(partitions):
            exp, fallback = gcs_utils.count_rows_for_prefix(
                f"{GCS_PREFIX_TRIPS}/dt={part}/"
            )
            act = bq_utils.count_partition(TABLE_TRIPS, date.fromisoformat(part))
            fallback_objects += len(fallback)
            total_expected += exp
            total_actual += act
            if act != exp:
                mismatches.append({"date": part, "gcs_rows": exp, "bq_rows": act})
                log.error("MISMATCH %s: GCS=%d BQ=%d", part, exp, act)

        summary = {
            "partitions": len(partitions),
            "datalake_rows": total_expected,
            "warehouse_rows": total_actual,
            "mismatches": mismatches,
            # >0 berarti ada objek lama tanpa metadata; jalankan ingestion penuh
            # sekali agar metadata terisi dan verifikasi kembali murah.
            "objects_read_via_fallback": fallback_objects,
        }
        log.info("Verifikasi ingestion: %s", summary)

        if mismatches:
            raise ValueError(
                f"Jumlah baris tidak cocok di {len(mismatches)} partisi: "
                f"{mismatches[:5]}"
            )
        return summary

    # --------------------------------------------------------
    # Wiring
    # --------------------------------------------------------
    files = discover_source_files()
    split = split_and_upload.expand(fname=files)

    # collect_partitions menerima hasil split sebagai argumen -> dependensi
    # eksplisit, dan menghindari kebutuhan operator `>>` pada XComArg.
    partitions = collect_partitions(split)
    loaded = load_partition.expand(partition=partitions)
    verified = verify_row_counts()

    loaded >> verified


citibike_ingest_trips()
