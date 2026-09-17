"""Utilitas BigQuery untuk DAG (raw layer load + verifikasi).

Prinsip desain
--------------
Tabel raw dipartisi berdasarkan **kolom** (``_ride_started_date`` /
``_snapshot_date``), bukan ingestion-time partitioning. Konsekuensinya:

    Partition decorator (``table$YYYYMMDD``) TIDAK didukung untuk tabel
    ber-partisi kolom — BigQuery menolaknya dengan pesan
    "Table ... cannot include decorator".

Karena itu idempotensi dicapai dengan pola **delete-then-append**:

    1. Hapus baris pada satu nilai partisi (partisi di-prune -> hanya
       partisi itu yang dipindai, murah).
    2. Load file Parquet dari GCS dengan ``WRITE_APPEND``.

Re-run menjadi idempoten: hasil akhir selalu sama dengan isi GCS.
Penghitungan baris per partisi memakai filter ``WHERE kolom_partisi``,
bukan decorator.
"""
from __future__ import annotations

import logging
from datetime import date

from google.cloud import bigquery

from common.config import LOCATION, PROJECT_ID

log = logging.getLogger(__name__)

_client: bigquery.Client | None = None


def client() -> bigquery.Client:
    """Client BigQuery singleton."""
    global _client
    if _client is None:
        _client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    return _client


# ============================================================
# Skema tabel raw.trips — HARUS sinkron dengan
# sql/bigquery/01_raw_tables_ddl.sql
# ============================================================
TRIPS_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("ride_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("rideable_type", "STRING"),
    bigquery.SchemaField("started_at", "TIMESTAMP"),
    bigquery.SchemaField("ended_at", "TIMESTAMP"),
    bigquery.SchemaField("start_station_name", "STRING"),
    bigquery.SchemaField("start_station_id", "STRING"),
    bigquery.SchemaField("end_station_name", "STRING"),
    bigquery.SchemaField("end_station_id", "STRING"),
    bigquery.SchemaField("start_lat", "FLOAT64"),
    bigquery.SchemaField("start_lng", "FLOAT64"),
    bigquery.SchemaField("end_lat", "FLOAT64"),
    bigquery.SchemaField("end_lng", "FLOAT64"),
    bigquery.SchemaField("member_casual", "STRING"),
    bigquery.SchemaField("_source_file", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("_ride_started_date", "DATE"),
]


PARTITION_COL_TRIPS = "_ride_started_date"


def delete_partition(
    table_id: str,
    partition_date: date,
    partition_column: str = PARTITION_COL_TRIPS,
) -> None:
    """Hapus seluruh baris pada satu nilai partisi.

    DML dengan filter pada kolom partisi -> BigQuery hanya memindai
    partisi tersebut (partition pruning), bukan seluruh tabel.
    """
    sql = (
        f"DELETE FROM `{table_id}` "
        f"WHERE {partition_column} = DATE '{partition_date.isoformat()}'"
    )
    log.info("Hapus partisi %s pada %s", partition_date, table_id)
    client().query(sql, location=LOCATION).result()


def load_parquet_partition(
    table_id: str,
    partition_date: date,
    source_uris: list[str],
    schema: list[bigquery.SchemaField],
    partition_column: str = PARTITION_COL_TRIPS,
) -> int:
    """Muat file Parquet dari GCS ke satu partisi tabel BigQuery (idempoten).

    Args:
        table_id: mis. ``project.dataset.trips``
        partition_date: tanggal partisi (dari kolom ``_ride_started_date``)
        source_uris: daftar ``gs://`` URI Parquet untuk tanggal tsb
        schema: skema eksplisit tujuan
        partition_column: kolom partisi tabel tujuan

    Returns:
        Jumlah baris yang berhasil dimuat.

    Urutan: hapus partisi tujuan -> append data baru. Pola ini dipakai
    karena decorator partisi tidak berlaku pada tabel ber-partisi kolom.
    """
    if not source_uris:
        log.warning("Tidak ada file untuk partisi %s — dilewati.", partition_date)
        return 0

    # 1) Bersihkan partisi tujuan agar re-run tidak menghasilkan duplikat.
    delete_partition(table_id, partition_date, partition_column)

    # 2) Append data dari datalake.
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    log.info("Load %d file ke %s partisi %s", len(source_uris), table_id, partition_date)
    load_job = client().load_table_from_uri(source_uris, table_id, job_config=job_config)
    load_job.result()  # tunggu sampai selesai

    rows = load_job.output_rows or 0
    log.info("Partisi %s selesai: %d baris", partition_date, rows)
    return rows


def query_scalar(sql: str) -> int | float | None:
    """Jalankan query dan ambil nilai baris pertama kolom pertama."""
    rows = list(client().query(sql, location=LOCATION).result())
    if not rows:
        return None
    return rows[0][0]


# ============================================================
# Skema tabel raw.station_information — HARUS sinkron dengan
# sql/bigquery/01_raw_tables_ddl.sql
# ============================================================
STATION_INFO_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("station_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("short_name", "STRING"),
    bigquery.SchemaField("lat", "FLOAT64"),
    bigquery.SchemaField("lon", "FLOAT64"),
    bigquery.SchemaField("capacity", "INT64"),
    bigquery.SchemaField("region_id", "STRING"),
    # mode REPEATED pada STRING sama dengan ARRAY<STRING> di BigQuery.
    bigquery.SchemaField("rental_methods", "STRING", mode="REPEATED"),
    bigquery.SchemaField("eightd_has_key_dispenser", "BOOL"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
]


def load_ndjson_replace(
    table_id: str,
    source_uris: list[str],
    schema: list[bigquery.SchemaField],
) -> int:
    """Replace penuh tabel dari berkas NDJSON di GCS.

    Dipakai untuk tabel referensi (station_information) yang tidak punya
    penulis lain: setiap run menimpa seluruh isi, sehingga re-run idempoten
    tanpa perlu logika dedup di layer dimensi.

    Format NDJSON dipilih karena payload GBFS memuat array (`rental_methods`)
    yang ditangani JSON secara native, berbeda dengan Parquet yang menuntut
    pemetaan tipe manual.
    """
    if not source_uris:
        log.warning("Tidak ada berkas untuk dimuat ke %s — dilewati.", table_id)
        return 0

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    log.info("Replace %s dari %d berkas NDJSON", table_id, len(source_uris))
    load_job = client().load_table_from_uri(source_uris, table_id, job_config=job_config)
    load_job.result()

    rows = load_job.output_rows or 0
    log.info("Tabel %s di-replace: %d baris", table_id, rows)
    return rows


def count_partition(
    table_id: str,
    partition_date: date,
    partition_column: str = PARTITION_COL_TRIPS,
) -> int:
    """Hitung baris pada partisi tanggal tertentu (verifikasi load).

    Memakai filter kolom partisi, BUKAN decorator — decorator tidak
    didukung pada tabel ber-partisi kolom.
    """
    sql = (
        f"SELECT COUNT(*) FROM `{table_id}` "
        f"WHERE {partition_column} = DATE '{partition_date.isoformat()}'"
    )
    return int(query_scalar(sql) or 0)
