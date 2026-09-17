"""Consumer: topik snapshot -> Parquet di GCS -> BigQuery.

Dua keputusan desain yang membedakannya dari pipeline batch
------------------------------------------------------------

1. **Append-only, bukan delete-partition.** Pipeline batch menerapkan
   *hapus partisi lalu append* karena setiap tanggal dimuat ulang menyeluruh.
   Di streaming itu justru berbahaya: consumer menulis terus-menerus, sehingga
   menghapus partisi tanggal ini akan menghapus data yang baru saja ditulis
   pada hari yang sama.

   Konsekuensinya duplikat mungkin tersimpan, dan itu **diterima**: Kafka
   bersifat *at-least-once*. Duplikat dirapikan di layer staging dbt dengan
   kunci `(station_id, last_reported)`.

2. **Offset di-commit setelah data benar-benar tersimpan.** Kalau consumer
   mati di tengah, pesan akan dibaca ulang — bukan hilang. Mengulang aman
   karena duplikat ditangani di hulu seperti poin 1.

Penulisan dilakukan **massal per batch** (bukan per pesan) karena satu load job
BigQuery per pesan akan sangat mahal dan lambat.
"""
from __future__ import annotations

import io
import json
import logging
import signal
import sys
import time
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.cloud import bigquery, storage

from streaming.config import (
    BOOTSTRAP_SERVERS,
    BUCKET_RAW,
    CONSUMER_BATCH_SIZE,
    CONSUMER_FLUSH_SECONDS,
    CONSUMER_GROUP,
    GCS_PREFIX_STATION_STATUS,
    LOCATION,
    PROJECT_ID,
    TABLE_DLQ,
    TABLE_STATION_STATUS,
    TOPIC_DLQ,
    TOPIC_SNAPSHOT,
)

log = logging.getLogger("snapshot_writer")

_shutdown = False

# Skema eksplisit supaya tipe kolom tidak hanyut mengikuti inferensi.
# HARUS sinkron dengan sql/bigquery/01_raw_tables_ddl.sql
STATION_STATUS_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("station_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("num_bikes_available", "INT64"),
    bigquery.SchemaField("num_ebikes_available", "INT64"),
    bigquery.SchemaField("num_scooters_available", "INT64"),
    bigquery.SchemaField("num_docks_available", "INT64"),
    bigquery.SchemaField("num_bikes_disabled", "INT64"),
    bigquery.SchemaField("num_docks_disabled", "INT64"),
    bigquery.SchemaField("is_installed", "BOOL"),
    bigquery.SchemaField("is_renting", "BOOL"),
    bigquery.SchemaField("is_returning", "BOOL"),
    bigquery.SchemaField("is_disabled", "BOOL"),
    bigquery.SchemaField("last_reported", "TIMESTAMP"),
    bigquery.SchemaField("snapshot_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("_snapshot_date", "DATE"),
]

DLQ_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("raw_payload", "STRING"),
    bigquery.SchemaField("error_reason", "STRING"),
    bigquery.SchemaField("kafka_topic", "STRING"),
    bigquery.SchemaField("kafka_partition", "INT64"),
    bigquery.SchemaField("kafka_offset", "INT64"),
    bigquery.SchemaField("failed_at", "TIMESTAMP"),
]

# Pemetaan tipe BigQuery -> Arrow, ditulis eksplisit agar tidak perlu
# menebak-nebak saat membaca ulang kode ini.
_BQ_TO_ARROW = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "BOOL": pa.bool_(),
    "TIMESTAMP": pa.timestamp("us"),
    "DATE": pa.date32(),
}


def _arrow_type_for(bq_type: str) -> pa.DataType:
    if bq_type not in _BQ_TO_ARROW:
        raise ValueError(f"Tipe BigQuery '{bq_type}' belum dipetakan ke Arrow.")
    return _BQ_TO_ARROW[bq_type]


def _parquet_schema(fields: list[bigquery.SchemaField]) -> pa.Schema:
    return pa.schema([(f.name, _arrow_type_for(f.field_type)) for f in fields])


PARQUET_SCHEMA = _parquet_schema(STATION_STATUS_SCHEMA)
PARQUET_DLQ_SCHEMA = _parquet_schema(DLQ_SCHEMA)


def _handle_signal(signum, _frame) -> None:
    global _shutdown
    _shutdown = True
    log.info("Menerima signal %s — menyelesaikan batch terakhir.", signum)


def _iso_to_ts(value: str | None) -> datetime | None:
    """Ubah string ISO menjadi datetime naif (kolom BigQuery TIMESTAMP)."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).replace(tzinfo=None)
    except ValueError:
        return None


def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            # Commit manual: offset hanya maju setelah data tersimpan.
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300_000,
        }
    )


def row_from_message(msg_value: bytes, *, kafka_meta: dict[str, Any]) -> dict:
    """Ubah satu pesan Kafka menjadi baris tabel.

    Pesan yang isinya tidak bisa dipakai tidak dibuang, melainkan ditandai
    untuk masuk DLQ.
    """
    try:
        payload = json.loads(msg_value)
    except json.JSONDecodeError:
        return {
            "__dlq__": True,
            "raw_payload": msg_value.decode(errors="replace")[:50_000],
            "error_reason": "invalid_json",
            **kafka_meta,
        }

    if not isinstance(payload, dict) or "station_id" not in payload:
        return {
            "__dlq__": True,
            "raw_payload": json.dumps(payload, ensure_ascii=False)[:50_000],
            "error_reason": "missing_station_id",
            **kafka_meta,
        }

    snapshot_dt = _iso_to_ts(payload.get("snapshot_timestamp")) or datetime.now(
        timezone.utc
    ).replace(tzinfo=None)

    return {
        "station_id": str(payload["station_id"]),
        "num_bikes_available": payload.get("num_bikes_available"),
        "num_ebikes_available": payload.get("num_ebikes_available"),
        "num_scooters_available": payload.get("num_scooters_available"),
        "num_docks_available": payload.get("num_docks_available"),
        "num_bikes_disabled": payload.get("num_bikes_disabled"),
        "num_docks_disabled": payload.get("num_docks_disabled"),
        "is_installed": payload.get("is_installed"),
        "is_renting": payload.get("is_renting"),
        "is_returning": payload.get("is_returning"),
        "is_disabled": payload.get("is_disabled"),
        "last_reported": _iso_to_ts(payload.get("last_reported_iso")),
        "snapshot_timestamp": snapshot_dt,
        "_ingested_at": datetime.now(timezone.utc).replace(tzinfo=None),
        "_snapshot_date": snapshot_dt.date(),
    }


def write_parquet_to_gcs(
    rows: list[dict],
    *,
    gcs_client: storage.Client,
    schema: pa.Schema,
    prefix: str,
    partition_date: date,
    tag: str,
) -> str | None:
    """Tulis batch sebagai Parquet ke GCS, kembalikan gs:// URI-nya."""
    if not rows:
        return None

    frame = pd.DataFrame(rows)
    for field in schema:
        if field.name not in frame.columns:
            frame[field.name] = None
    frame = frame[[f.name for f in schema]]

    table = pa.Table.from_pandas(frame, schema=schema, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")

    stamp = datetime.now(timezone.utc).strftime("%H%M%S%f")
    blob_name = f"{prefix}/dt={partition_date.isoformat()}/snapshot-{stamp}-{tag}.parquet"

    blob = gcs_client.bucket(BUCKET_RAW).blob(blob_name)
    # Jumlah baris disimpan sebagai metadata supaya verifikasi bisa dihitung
    # tanpa mengunduh berkas Parquet-nya.
    blob.metadata = {"rows": str(len(rows))}
    blob.upload_from_string(buffer.getvalue(), content_type="application/octet-stream")

    uri = f"gs://{BUCKET_RAW}/{blob_name}"
    log.info("Tulis %d baris -> %s", len(rows), uri)
    return uri


def load_to_bigquery(bq_client: bigquery.Client, uri: str, schema, table_id: str) -> int:
    """Append satu berkas Parquet dari GCS ke tabel BigQuery."""
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    job.result()
    return job.output_rows or 0


def flush_batch(
    *,
    rows: list[dict],
    dlq_rows: list[dict],
    gcs_client: storage.Client,
    bq_client: bigquery.Client,
) -> None:
    """Tulis satu batch ke datalake lalu ke warehouse."""
    if rows:
        # Dikelompokkan per tanggal agar penulisan GCS rapi dan partisi
        # BigQuery menyesuaikan dengan _snapshot_date.
        by_date: dict[date, list[dict]] = {}
        for row in rows:
            by_date.setdefault(row["_snapshot_date"], []).append(row)

        for partition_date, part_rows in sorted(by_date.items()):
            uri = write_parquet_to_gcs(
                part_rows,
                gcs_client=gcs_client,
                schema=PARQUET_SCHEMA,
                prefix=GCS_PREFIX_STATION_STATUS,
                partition_date=partition_date,
                tag=f"n{len(part_rows)}",
            )
            if uri:
                loaded = load_to_bigquery(
                    bq_client, uri, STATION_STATUS_SCHEMA, TABLE_STATION_STATUS
                )
                log.info(
                    "BigQuery station_status: +%d baris (partisi %s)",
                    loaded,
                    partition_date,
                )

    if dlq_rows:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for row in dlq_rows:
            row.pop("__dlq__", None)
            row.setdefault("failed_at", now)
        uri = write_parquet_to_gcs(
            dlq_rows,
            gcs_client=gcs_client,
            schema=PARQUET_DLQ_SCHEMA,
            prefix=f"{GCS_PREFIX_STATION_STATUS}/_rejected",
            partition_date=now.date(),
            tag=f"dlq{len(dlq_rows)}",
        )
        if uri:
            loaded = load_to_bigquery(bq_client, uri, DLQ_SCHEMA, TABLE_DLQ)
            log.warning("BigQuery DLQ: +%d baris bermasalah", loaded)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    log.info(
        "Consumer mulai. group=%s, batch=%d, flush=%ss",
        CONSUMER_GROUP,
        CONSUMER_BATCH_SIZE,
        CONSUMER_FLUSH_SECONDS,
    )

    consumer = build_consumer()
    consumer.subscribe([TOPIC_SNAPSHOT, TOPIC_DLQ])

    gcs_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)

    rows: list[dict] = []
    dlq_rows: list[dict] = []
    # Waktu pesan terakhir diterima. Dipakai untuk flush berbasis IDLE:
    # penulisan dilakukan ketika pesan berhenti mengalir, bukan berdasarkan
    # jam. Kalau memakai jam (periodik), pesan pertama yang datang setelah
    # masa sepi akan langsung memicu flush sendirian — dan sisa ribuan pesan
    # snapshot menunggu timer berikutnya. Akibatnya muncul berkas Parquet
    # berisi 1 baris, ditambah satu load job BigQuery tambahan per snapshot.
    last_message_at = time.monotonic()
    pending_offsets = False

    try:
        while not _shutdown:
            msg = consumer.poll(timeout=1.0)
            got_message = False

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(msg.error())
            else:
                got_message = True
                kafka_meta = {
                    "kafka_topic": msg.topic(),
                    "kafka_partition": msg.partition(),
                    "kafka_offset": msg.offset(),
                }
                if msg.topic() == TOPIC_DLQ:
                    dlq_rows.append(
                        {
                            "__dlq__": True,
                            "raw_payload": (msg.value() or b"")
                            .decode(errors="replace")[:50_000],
                            "error_reason": "producer_dlq",
                            **kafka_meta,
                        }
                    )
                else:
                    row = row_from_message(msg.value() or b"", kafka_meta=kafka_meta)
                    if row.get("__dlq__"):
                        dlq_rows.append(row)
                    else:
                        rows.append(row)
                pending_offsets = True

            if got_message:
                last_message_at = time.monotonic()

            # Tulis bila buffer penuh, ATAU bila sudah tidak ada pesan baru
            # selama CONSUMER_FLUSH_SECONDS sehingga buffer tidak mengendap.
            buffer_size = len(rows) + len(dlq_rows)
            idle_seconds = time.monotonic() - last_message_at
            should_flush = (
                buffer_size >= CONSUMER_BATCH_SIZE
                or (buffer_size > 0 and idle_seconds >= CONSUMER_FLUSH_SECONDS)
            )
            if should_flush:
                flush_batch(
                    rows=rows,
                    dlq_rows=dlq_rows,
                    gcs_client=gcs_client,
                    bq_client=bq_client,
                )
                # Offset baru dimajukan SETELAH data tersimpan.
                consumer.commit(asynchronous=False)
                rows, dlq_rows = [], []
                pending_offsets = False
                last_message_at = time.monotonic()

        # Saat shutdown, tulis sisa buffer lalu commit.
        if rows or dlq_rows:
            flush_batch(
                rows=rows,
                dlq_rows=dlq_rows,
                gcs_client=gcs_client,
                bq_client=bq_client,
            )
            consumer.commit(asynchronous=False)
            pending_offsets = False
    finally:
        if pending_offsets:
            log.warning(
                "Ada offset yang belum di-commit — pesan tersebut akan "
                "diproses ulang pada start berikutnya (aman, duplikat "
                "ditangani di layer staging)."
            )
        consumer.close()
        log.info("Consumer berhenti dengan bersih.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
