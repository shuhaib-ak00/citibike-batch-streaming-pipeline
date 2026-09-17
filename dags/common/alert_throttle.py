"""Throttle pengiriman alert agar tidak menabrak batas Slack.

Masalah yang diselesaikan
-------------------------
Slack Incoming Webhook membatasi sekitar **1 pesan per detik** per webhook.
Pipeline ini punya beberapa task yang berjalan paralel (mis. 91 task
`load_partition` pada ingestion). Bila kegagalannya sistemik — kredensial
GCP dicabut, jaringan mati — puluhan task akan gagal hampir bersamaan dan
mengirim alert masing-masing. Sebagian besar akan dibuang diam-diam oleh
Slack, sehingga jumlah alert yang terlihat jauh lebih sedikit daripada
jumlah kegagalan sebenarnya.

Pendekatan
----------
Dua lapis, dan keduanya diperlukan:

1. **Deduplikasi berjendela** (`should_send`). Alert dengan kunci sama
   (mis. dag+task yang sama) tidak dikirim lebih dari sekali dalam rentang
   `ALERT_DEDUP_WINDOW_SECONDS`. Ini menahan badai alert dari task identik
   yang di-retry berulang.

2. **Pembatas laju** (`rate_limit_ok`). Minimal ada jeda
   `ALERT_MIN_INTERVAL_SECONDS` antar pengiriman ke webhook, apa pun
   kuncinya. Ini menjaga batas keras Slack.

Keduanya disimpan di **database Airflow**, bukan di memori proses. Alasannya
penting: task Airflow berjalan di proses terpisah (scheduler, worker), jadi
state di memori tidak akan terlihat antar task. Menyimpan di database membuat
throttle benar-benar berlaku lintas task.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.models import Variable

log = logging.getLogger(__name__)

# Jendela deduplikasi untuk alert dengan kunci sama (detik).
#
# Nilainya sengaja dibuat >= WATCHDOG_FAILED_RUN_LOOKBACK_MINUTES (60 menit)
# di watchdog_pipeline. Alasannya: watchdog mencari DAG-run gagal dalam
# jendela 60 menit dan berjalan tiap 10 menit. Bila jendela dedup lebih
# pendek, satu run gagal yang sama akan dilaporkan berulang kali — teramati
# 3 kali per jam sebelum nilainya diselaraskan. Dengan keduanya sama, tiap
# kegagalan dilaporkan tepat sekali, sementara alert per task harian tetap
# terkirim setiap hari karena jaraknya jauh lebih besar dari satu jam.
DEDUP_WINDOW_SECONDS = int(os.getenv("ALERT_DEDUP_WINDOW_SECONDS", "3600"))
# Jeda minimum antar pengiriman ke webhook (detik). Slack membatasi
# sekitar 1 pesan/detik; nilai ini sengaja lebih longgar.
MIN_INTERVAL_SECONDS = int(os.getenv("ALERT_MIN_INTERVAL_SECONDS", "3"))
# Pembatas badai alert. Saat kegagalan sistemik, puluhan task bisa gagal
# dalam beberapa detik (mis. 91 task load_partition sekaligus). Dedup tidak
# menolong karena kuncinya berbeda per task, sehingga tanpa pembatas ini
# Slack akan menerima puluhan pesan.
BURST_WINDOW_SECONDS = int(os.getenv("ALERT_BURST_WINDOW_SECONDS", "60"))
BURST_MAX_ALERTS = int(os.getenv("ALERT_BURST_MAX_ALERTS", "5"))

_VAR_LAST_SENT = "alert_last_sent_at"
_VAR_LAST_KEY = "alert_last_keys"
_VAR_BURST = "alert_burst_history"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _get_var(key: str, default):
    try:
        return Variable.get(key, default_var=default, deserialize_json=True)
    except Exception as exc:  # noqa: BLE001
        log.warning("Tidak bisa membaca Variable '%s': %s", key, exc)
        return default


def _set_var(key: str, value) -> None:
    try:
        Variable.set(key, value, serialize_json=True)
    except Exception as exc:  # noqa: BLE001
        log.warning("Tidak bisa menyimpan Variable '%s': %s", key, exc)


def should_send(dedup_key: str) -> bool:
    """True bila alert dengan kunci ini belum dikirim dalam jendela dedup.

    Kunci kosong selalu diizinkan — dipakai untuk alert yang memang harus
    selalu terkirim (mis. ringkasan kegagalan sebuah DAG-run).
    """
    if not dedup_key:
        return True

    state = _get_var(_VAR_LAST_KEY, {}) or {}
    last = state.get(dedup_key)
    if last:
        try:
            last_dt = datetime.fromisoformat(last)
            if _now() - last_dt < timedelta(seconds=DEDUP_WINDOW_SECONDS):
                log.info(
                    "Alert '%s' ditahan (dedup %ss, terakhir %s).",
                    dedup_key, DEDUP_WINDOW_SECONDS, last,
                )
                return False
        except ValueError:
            pass  # format lama/rusak -> anggap belum pernah dikirim

    # Bersihkan entri kedaluwarsa agar Variable tidak tumbuh tanpa batas.
    batas = _now() - timedelta(seconds=DEDUP_WINDOW_SECONDS * 4)
    bersih = {}
    for k, v in state.items():
        try:
            if datetime.fromisoformat(v) >= batas:
                bersih[k] = v
        except ValueError:
            continue
    bersih[dedup_key] = _now().isoformat()
    _set_var(_VAR_LAST_KEY, bersih)
    return True


def rate_limit_ok() -> bool:
    """True bila sudah cukup lama sejak pengiriman terakhir ke webhook.

    Dipanggil tepat sebelum mengirim. Bila belum cukup lama, pemanggil
    disarankan **tidak** menunggu (blocking) — cukup dicatat di log, karena
    task yang menunggu akan memperlambat pipeline hanya demi notifikasi.
    """
    last = _get_var(_VAR_LAST_SENT, None)
    if last:
        try:
            if _now() - datetime.fromisoformat(last) < timedelta(
                seconds=MIN_INTERVAL_SECONDS
            ):
                log.info("Rate limit: pengiriman terakhir %s, ditahan.", last)
                return False
        except ValueError:
            pass
    _set_var(_VAR_LAST_SENT, _now().isoformat())
    return True


def burst_guard_ok() -> bool:
    """Batasi jumlah alert dalam jendela waktu pendek.

    Deduplikasi tidak menolong saat kegagalan sistemik: kunci dedup berbeda
    per task, sehingga 91 task yang gagal bersamaan tetap menghasilkan 91
    pesan. Fungsi ini memberi batas global — setelah `BURST_MAX_ALERTS`
    pesan dalam `BURST_WINDOW_SECONDS`, sisanya ditahan.

    Hanya dipakai untuk alert kegagalan task. Alert ringkasan DAG-run dan
    peringatan watchdog **melewati** pembatas ini, karena justru merekalah
    yang memberi gambaran lengkap saat badai alert terjadi.
    """
    history = _get_var(_VAR_BURST, []) or []
    sekarang = _now()

    masih_relevan = []
    for t in history:
        try:
            if (sekarang - datetime.fromisoformat(t)).total_seconds() < BURST_WINDOW_SECONDS:
                masih_relevan.append(t)
        except (ValueError, TypeError):
            continue

    if len(masih_relevan) >= BURST_MAX_ALERTS:
        log.warning(
            "Burst guard aktif: %d alert dalam %ss terakhir (batas %d). "
            "Alert kegagalan task ditahan; ringkasan DAG-run tetap dikirim.",
            len(masih_relevan), BURST_WINDOW_SECONDS, BURST_MAX_ALERTS,
        )
        return False

    masih_relevan.append(sekarang.isoformat())
    _set_var(_VAR_BURST, masih_relevan)
    return True
