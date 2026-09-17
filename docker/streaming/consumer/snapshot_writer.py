"""Consumer: Kafka -> Parquet di GCS -> BigQuery.

Kenapa **append-only** (bukan delete-partition seperti pipeline batch):
consumer menulis terus-menerus sepanjang hari. Menghapus partisi tanggal ini
sebelum menulis batch berikutnya akan menghapus data yang baru saja ditulis
pada hari yang sama.

Duplikat ditangani di layer staging dbt dengan kunci
`(station_id, last_reported)` — konsisten dengan sifat at-least-once Kafka.

Offset Kafka baru di-commit **setelah** penulisan ke BigQuery berhasil,
sehingga crash tidak menghilangkan data (mungkin mengulang, dan itu aman
karena idempotensi ditangani di hulu).
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
    DATASET_RAW,
    GCS_PREFIX_STATION_STATUS,
    GCS_PREFIX_STATION_STATUS_REJECTED,
    LOCATION,
    PROJECT_ID,
    TABLE_DLQ,
    TABLE_STATION_STATUS,
    TOPIC_DLQ,
    TOPIC_SNAPSHOT,
)

log = logging.getLogger("snapshot_writer")

_shutdown = False

# Skema eksplisit supaya tipe kolom tidak hanyut mengikuti tebakan inferensi.
STATION_STATUS_SCHEMA = [
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

DLQ_SCHEMA = [
    bigquery.SchemaField("raw_payload", "STRING"),
    bigquery.SchemaField("error_reason", "STRING"),
    bigquery.SchemaField("kafka_topic", "STRING"),
    bigquery.SchemaField("kafka_partition", "INT64"),
    bigquery.SchemaField("kafka_offset", "INT64"),
    bigquery.SchemaField("failed_at", "TIMESTAMP"),
]

PARQUET_SCHEMA = pa.schema(
    [(f.name, pa.string() if f.field_type == "STRING"
      else pa.int64() if f.field_type == "INT64"
      else pa.bool_() if f.field_type == "BOOL"
      else pa.timestamp("us") if f.field_type == "TIMESTAMP"
      else pa.date32())
     for f in STATION_STATUS_SCHEMA]
)


def _handle_signal(signum, _frame) -> None:
    global _shutdown
    _shutdown = True
    log.info("Menerima signal %s — menyelesaikan batch terakhir.", signum)


def _iso_to_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value).replace(tzinfo=None)


def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            # Commit manual: offset hanya maju setelah data benar-benar tersimpan.
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300_000,
        }
    )


def rows_from_message(msg_value: bytes, *, kafka_meta: dict[str, Any]) -> dict | None:
    """Ubah satu pesan Kafka menjadi baris tabel (None bila bukan snapshot)."""
    try:
        payload = json.loads(msg_value)
    except json.JSONDecodeError:
        return {"__dlq__": True, "raw_payload": msg_value.decode(errors="replace"),
                "error_reason": "invalid_json", **kafka_meta}

    if "station_id" not in payload:
        return {"__dlq__": True, "raw_payload": json.dumps(payload),
                "error_reason": "missing_station_id", **kafka_meta}

    snapshot_dt = _iso_to_ts(payload.get("snapshot_timestamp")) or datetime.now(timezone.utc).replace(tzinfo=None)

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
    rows: list[dict], *, gcs_client: storage.Client, partition_date: date, tag: str
) -> str | None:
    """Tulis batch sebagai Parquet ke GCS dan kembalikan gs:// URI."""
    if not rows:
        return None

    frame = pd.DataFrame(rows)
    for field in PARQUET_SCHEMA:
        if field.name not in frame.columns:
            frame[field.name] = None
    frame = frame[[f.name for f in PARQUET_SCHEMA]]

    table = pa.Table.from_pandas(frame, schema=PARQUET_SCHEMA, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")

    stamp = datetime.now(timezone.utc).strftime("%H%M%S%f")
    blob_name = (
        f"{GCS_PREFIX_STATION_STATUS}/dt={partition_date.isoformat()}/"
        f"snapshot-{stamp}-{tag}.parquet"
    )
    blob = gcs_client.bucket(BUCKET_RAW).blob(blob_name)
    # Jumlah baris disimpan sebagai metadata agar verifikasi bisa dihitung
    # tanpa mengunduh berkas Parquet-nya.
    blob.metadata = {"rows": str(len(rows))}
    blob.upload_from_string(buffer.getvalue(), content_type="application/octet-stream")
    log.info("Tulis %d baris -> gs://%s/%s", len(rows), BUCKET_RAW, blob_name)
    return f"gs://{BUCKET_RAW}/{blob_name}"


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
        # Partisi dikelompokkan per tanggal agar penulisan GCS rapi.
        by_date: dict[date, list[dict]] = {}
        for row in rows:
            by_date.setdefault(row["_snapshot_date"], []).append(row)

        for partition_date, part_rows in by_date.items():
            uri = write_parquet_to_gcs(
                part_rows,
                gcs_client=gcs_client,
                partition_date=partition_date,
                tag=f"n{len(part_rows)}",
            )
            if uri:
                loaded = load_to_bigquery(
                    bq_client, uri, STATION_STATUS_SCHEMA, TABLE_STATION_STATUS
                )
                log.info("BigQuery station_status: +%d baris (partisi %s)", loaded, partition_date)

    if dlq_rows:
        for row in dlq_rows:
            row.pop("__dlq__", None)
            row.setdefault("failed_at", datetime.now(timezone.utc).replace(tzinfo=None))
        uri = write_parquet_to_gcs(
            dlq_rows,
            gcs_client=gcs_client,
            partition_date=date.today(),
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
    last_flush = time.monotonic()
    pending_offsets = False

    try:
        while not _shutdown:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(msg.error())
            else:
                kafka_meta = {
                    "kafka_topic": msg.topic(),
                    "kafka_partition": msg.partition(),
                    "kafka_offset": msg.offset(),
                }
                if msg.topic() == TOPIC_DLQ:
                    dlq_rows.append({
                        "__dlq__": True,
                        "raw_payload": (msg.value() or b"").decode(errors="replace")[:50_000],
                        "error_reason": "producer_dlq",
                        **kafka_meta,
                    })
                else:
                    row = rows_from_message(msg.value() or b"", kafka_meta=kafka_meta)
                    if row is None:
                        pass
                    elif row.get("__dlq__"):
                        dlq_rows.append(row)
                    else:
                        rows.append(row)
                pending_offsets = True

            should_flush = (
                len(rows) + len(dlq_rows) >= CONSUMER_BATCH_SIZE
                or time.monotonic() - last_flush >= CONSUMER_FLUSH_SECONDS
            )
            if should_flush and (rows or dlq_rows):
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
                last_flush = time.monotonic()
            else:
                last_flush = last_flush if should_flush else last_flush

        # Saat shutdown, tulis sisa buffer lalu commit.
        if rows or dlq_rows:
            flush_batch(
                rows=rows, dlq_rows=dlq_rows, gcs_client=gcs_client, bq_client=bq_client
            )
            consumer.commit(asynchronous=False)
            pending_offsets = False
    finally:
        if pending_offsets:
            log.warning("Ada offset yang belum di-commit — data akan diproses ulang.")
        consumer.close()
        log.info("Consumer berhenti dengan bersih.")

    return 0


if __name__ == "__main__":
    sys.exit(main())