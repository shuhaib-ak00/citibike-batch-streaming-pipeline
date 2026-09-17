"""Producer: poll GBFS station_status -> topik Kafka.

Setiap polling mem-publish snapshot PENUH seluruh stasiun (periodic snapshot,
bukan CDC), sehingga tidak perlu menyimpan state per stasiun.

Catatan pengembangan:

- Pesan dipartisi dengan key ``station_id``; semua pesan satu stasiun masuk
  partisi yang sama sehingga urutannya konsisten. Urutan antar stasiun tidak
  dijamin dan tidak dibutuhkan.

- Payload yang gagal validasi dikirim ke topik DLQ, bukan dibuang -- baik
  kegagalan tingkat envelope maupun per stasiun. Satu stasiun rusak tidak
  menggagalkan seluruh snapshot.

- Kegagalan HTTP TIDAK masuk DLQ: itu masalah konektivitas, bukan data.
  Producer cukup mencoba lagi pada siklus berikutnya.
"""
from __future__ import annotations

import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from pydantic import ValidationError

from streaming.config import (
    BOOTSTRAP_SERVERS,
    HTTP_TIMEOUT_SECONDS,
    POLL_INTERVAL_SECONDS,
    STATION_STATUS_URL,
    TOPIC_DLQ,
    TOPIC_SNAPSHOT,
)
from streaming.schemas import GBFSEnvelope, StationStatus

log = logging.getLogger("gbfs_poller")

# Diisi oleh handler SIGTERM/SIGINT, diperiksa di dalam loop.
_shutdown = False


def _handle_signal(signum, _frame) -> None:
    global _shutdown
    _shutdown = True
    log.info("Menerima signal %s — berhenti setelah siklus ini selesai.", signum)


def _delivery_report(err, msg) -> None:
    """Dipanggil Kafka saat pengiriman selesai atau gagal."""
    if err is not None:
        log.error("Gagal mengirim pesan ke %s: %s", msg.topic(), err)


def build_producer() -> Producer:
    """Producer dengan setelan aman untuk pengiriman at-least-once."""
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            # Batasi retry agar producer tidak menggantung saat broker mati.
            "message.timeout.ms": 30_000,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
            "linger.ms": 50,
            "compression.type": "snappy",
        }
    )


def fetch_station_status() -> dict:
    """Ambil payload mentah station_status.json."""
    resp = requests.get(
        STATION_STATUS_URL,
        timeout=HTTP_TIMEOUT_SECONDS,
        headers={"User-Agent": "citibike-pipeline/1.0"},
    )
    resp.raise_for_status()
    return resp.json()


def publish_dlq(producer: Producer, *, raw: str, reason: str, detected_at: str) -> None:
    """Kirim payload bermasalah ke topik DLQ."""
    payload = {
        "raw_payload": raw[:50_000],  # batasi agar pesan Kafka tidak membengkak
        "error_reason": reason,
        "detected_at": detected_at,
    }
    producer.produce(
        TOPIC_DLQ,
        key="dlq",
        value=json.dumps(payload, ensure_ascii=False),
        callback=_delivery_report,
    )


def poll_once(producer: Producer) -> tuple[int, int]:
    """Satu siklus polling.

    Returns:
        (jumlah stasiun terkirim, jumlah stasiun ditolak)
    """
    snapshot_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    detected_at = snapshot_ts.isoformat()

    try:
        raw = fetch_station_status()
    except Exception as exc:  # noqa: BLE001
        # Kegagalan konektivitas, bukan kegagalan data — tidak masuk DLQ.
        log.error("Gagal mengambil station_status: %s", exc)
        return (0, 0)

    try:
        envelope = GBFSEnvelope.model_validate(raw)
    except ValidationError as exc:
        log.error("Envelope GBFS tidak valid: %s", exc)
        publish_dlq(
            producer,
            raw=json.dumps(raw)[:50_000],
            reason=f"invalid_envelope: {exc.error_count()} masalah",
            detected_at=detected_at,
        )
        return (0, 1)

    n_ok = 0
    n_bad = 0
    for item in envelope.data.stations:
        try:
            station = StationStatus.model_validate(item)
        except ValidationError as exc:
            n_bad += 1
            publish_dlq(
                producer,
                raw=json.dumps(item, ensure_ascii=False),
                reason=f"invalid_station: {exc.errors()[0].get('msg', 'unknown')}",
                detected_at=detected_at,
            )
            continue

        # Timestamp dikirim sebagai ISO string supaya consumer tidak perlu
        # tahu format epoch yang dipakai GBFS.
        message = {
            **station.model_dump(),
            "last_reported_iso": (
                station.last_reported_dt.isoformat()
                if station.last_reported_dt
                else None
            ),
            "snapshot_timestamp": snapshot_ts.isoformat(),
        }
        producer.produce(
            TOPIC_SNAPSHOT,
            key=station.station_id,
            value=json.dumps(message),
            callback=_delivery_report,
        )
        n_ok += 1

    producer.poll(0)  # layani callback pengiriman
    producer.flush(timeout=30)

    log.info(
        "Snapshot %s: %d stasiun terkirim, %d ditolak (total %d)",
        snapshot_ts.isoformat(timespec="seconds"),
        n_ok,
        n_bad,
        len(envelope.data.stations),
    )
    return (n_ok, n_bad)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    log.info(
        "Producer mulai. interval=%ss, topik=%s, sumber=%s",
        POLL_INTERVAL_SECONDS,
        TOPIC_SNAPSHOT,
        STATION_STATUS_URL,
    )
    producer = build_producer()

    while not _shutdown:
        started = time.monotonic()
        try:
            poll_once(producer)
        except Exception:  # noqa: BLE001
            # Satu siklus gagal tidak boleh mematikan producer.
            log.exception("Siklus polling gagal — lanjut ke siklus berikutnya.")

        # Kurangi waktu tidur dengan durasi siklus agar interval tetap stabil
        # walau pengambilan data memakan waktu.
        elapsed = time.monotonic() - started
        sisa = max(1.0, POLL_INTERVAL_SECONDS - elapsed)
        for _ in range(int(sisa)):
            if _shutdown:
                break
            time.sleep(1)

    producer.flush(timeout=30)
    log.info("Producer berhenti dengan bersih.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
