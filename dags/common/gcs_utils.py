"""Utilitas GCS untuk DAG (datalake raw layer)."""
from __future__ import annotations

import io
import logging
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
from google.cloud import storage

from common.config import BUCKET_RAW, PROJECT_ID

log = logging.getLogger(__name__)

# Nama kunci custom metadata yang menyimpan jumlah baris tiap objek Parquet.
ROW_COUNT_METADATA_KEY = "rows"

_client: storage.Client | None = None


def client() -> storage.Client:
    """Client GCS singleton (kredensial dari GOOGLE_APPLICATION_CREDENTIALS)."""
    global _client
    if _client is None:
        _client = storage.Client(project=PROJECT_ID)
    return _client


def bucket():
    return client().bucket(BUCKET_RAW)


def upload_file(
    local_path: str | Path,
    blob_name: str,
    metadata: dict[str, object] | None = None,
) -> str:
    """Upload satu file ke GCS. Mengembalikan gs:// URI.

    Args:
        local_path: berkas lokal yang di-upload.
        blob_name: path tujuan di dalam bucket.
        metadata: custom metadata yang menempel pada objek. Dipakai peng-upload
            Parquet untuk mencatat jumlah baris (``rows``) sehingga verifikasi
            bisa menghitung isi datalake **tanpa mengunduh** berkasnya.
    """
    local_path = Path(local_path)
    blob = bucket().blob(blob_name)
    if metadata:
        # Nilai metadata GCS harus string.
        blob.metadata = {str(k): str(v) for k, v in metadata.items()}
    blob.upload_from_filename(str(local_path))
    uri = f"gs://{BUCKET_RAW}/{blob_name}"
    log.info("Upload %s -> %s (%d bytes)", local_path.name, uri, local_path.stat().st_size)
    return uri


def _rows_from_parquet(blob: storage.Blob) -> int:
    """Hitung baris dengan membaca footer Parquet (butuh unduh berkas).

    Hanya dipakai sebagai *fallback* untuk objek lama yang belum punya
    custom metadata `rows`.
    """
    data = blob.download_as_bytes()
    return int(pq.read_metadata(io.BytesIO(data)).num_rows)


def count_rows_for_prefix(prefix: str) -> tuple[int, list[str]]:
    """Jumlahkan baris seluruh objek Parquet di bawah prefix.

    Prioritas: custom metadata ``rows`` (murah — hanya metadata, tanpa unduh).
    Bila tidak ada (objek dari run sebelum fitur ini), fallback membaca footer
    Parquet dan mencatat namanya di daftar ``fallback_read`` supaya bisa
    ditindaklanjuti (mis. jalankan ulang ingestion agar metadata terisi).

    Returns:
        (total_baris, daftar_nama_objek_yang_dibaca_via_fallback)
    """
    if not prefix.endswith("/"):
        prefix += "/"
    total = 0
    fallback_read: list[str] = []
    for blob in client().list_blobs(BUCKET_RAW, prefix=prefix):
        raw = (blob.metadata or {}).get(ROW_COUNT_METADATA_KEY)
        if raw is not None:
            total += int(raw)
        else:
            fallback_read.append(blob.name)
            total += _rows_from_parquet(blob)

    if fallback_read:
        log.warning(
            "prefix=%s: %d objek tanpa metadata '%s' — dibaca via footer Parquet. "
            "Jalankan ingestion penuh sekali agar metadata terisi.",
            prefix, len(fallback_read), ROW_COUNT_METADATA_KEY,
        )
    return total, fallback_read


def list_blobs(prefix: str) -> list[str]:
    """Daftar nama blob di bawah prefix (recursive).

    `prefix` harus diakhiri '/' agar cocok sebagai folder.
    """
    if not prefix.endswith("/"):
        prefix += "/"
    names = [b.name for b in client().list_blobs(BUCKET_RAW, prefix=prefix)]
    log.info("list_blobs prefix=%s -> %d objek", prefix, len(names))
    return sorted(names)


def list_prefixes(prefix: str, delimiter: str = "/") -> list[str]:
    """Daftar sub-prefix (folder) langsung di bawah prefix.

    Dipakai untuk menemukan partisi `dt=YYYY-MM-DD/` tanpa membaca isinya.
    """
    if not prefix.endswith("/"):
        prefix += "/"
    iterator = client().list_blobs(BUCKET_RAW, prefix=prefix, delimiter=delimiter)
    for _ in iterator:  # wajib diiterasi agar prefixes terisi
        pass
    return sorted(iterator.prefixes)


def newest_blob_age_seconds(prefix: str) -> float | None:
    """Umur (detik) objek terbaru di bawah prefix, atau None bila kosong.

    Dipakai watchdog streaming untuk memeriksa apakah consumer masih menulis.
    Memakai `updated` (waktu objek selesai ditulis), bukan `time_created`,
    karena objek yang ditimpa ulang tetap dihitung sebagai aktivitas baru.

    Pemeriksaan ini **berbeda** dari freshness end-to-end: umur objek di GCS
    menunjukkan consumer masih bekerja, sedangkan umur `snapshot_timestamp`
    di BigQuery menunjukkan seluruh rantai (producer -> Kafka -> consumer ->
    load) berjalan. Bila keduanya berbeda jauh, letak masalahnya bisa
    dipersempit: consumer hidup tetapi load ke BigQuery gagal.
    """
    if not prefix.endswith("/"):
        prefix += "/"

    terbaru = None
    for blob in client().list_blobs(BUCKET_RAW, prefix=prefix):
        if blob.updated is None:
            continue
        if terbaru is None or blob.updated > terbaru:
            terbaru = blob.updated

    if terbaru is None:
        return None
    return (datetime.now(timezone.utc) - terbaru).total_seconds()


def download_file(blob_name: str, local_path: str | Path) -> Path:
    """Unduh blob ke path lokal."""
    local_path = Path(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    bucket().blob(blob_name).download_to_filename(str(local_path))
    return local_path
