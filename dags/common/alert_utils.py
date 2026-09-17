"""Mekanisme alert kegagalan pipeline.

Ruang lingkupnya sengaja sempit: **alert otomatis hanya untuk kegagalan
pipeline**. Risiko operasional stasiun (empty/full) tidak dikirim sebagai
notifikasi, melainkan ditampilkan sebagai indikator visual di dashboard —
itu keputusan desain, bukan keterbatasan.

Dua lapis keluaran:

1. **Log terstruktur** (selalu aktif) — JSON mudah di-grep dan muncul di UI
   Airflow pada task yang gagal.
2. **Webhook Slack** (opsional) — aktif begitu ``ALERT_WEBHOOK_URL`` diisi di
   ``.env``. Karena Slack Incoming Webhook mewajibkan payload ber-key
   ``text``, payload mentah tidak bisa dikirim apa adanya (Slack merespons
   ``400 invalid_payload``). ``_send_webhook`` karena itu menyusun pesan teks
   yang terbaca manusia untuk Slack, dan mengirim payload JSON utuh hanya ke
   webhook generik.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from airflow.models import DagRun, TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import State

from common import alert_throttle

log = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _is_slack(url: str) -> bool:
    return "hooks.slack.com" in url


def _format_alert_text(payload: dict[str, Any]) -> str:
    """Susun pesan alert yang terbaca manusia untuk Slack.

    Menangani empat bentuk payload: kegagalan task (``notify``), ringkasan
    kegagalan DAG-run (``alert_dag_run_failed``), lonjakan karantina
    (``alert_data_quality``), dan peringatan watchdog streaming
    (``alert_streaming``).
    """
    jenis = payload.get("alert_type")

    if jenis == "DAG_RUN_FAILURE":
        judul, ikon = "DAG-run GAGAL", ":rotating_light:"
        gagal = payload.get("failed_tasks") or []
        # Batasi daftarnya: kegagalan sistemik bisa melibatkan puluhan task,
        # dan pesan Slack yang terlalu panjang justru menyulitkan dibaca.
        tampil = ", ".join(f"`{t}`" for t in gagal[:10])
        if len(gagal) > 10:
            tampil += f" … (+{len(gagal) - 10} lainnya)"
        baris = [
            f"{ikon} *{judul}* — {payload.get('severity', 'ERROR')}",
            f"• *DAG*: `{payload.get('dag_id', '-')}`",
            f"• *Run*: `{payload.get('run_id', '-')}`",
            f"• *Task gagal*: {payload.get('failed_count', 0)} dari "
            f"{payload.get('total_tasks', 0)}",
            f"• *Daftar*: {tampil or '-'}",
            f"• *Waktu*: {payload.get('detected_at', '-')}",
        ]
        return "\n".join(baris)

    if jenis == "STREAMING_WARNING":
        baris = [
            f":warning: *{payload.get('title', 'Peringatan streaming')}* "
            f"— {payload.get('severity', 'WARNING')}",
            f"• *Pemeriksaan*: {payload.get('check', '-')}",
            f"• *Temuan*: {payload.get('detail', '-')}",
            f"• *Ambang*: {payload.get('threshold', '-')}",
            f"• *Nilai*: {payload.get('observed', '-')}",
            f"• *Waktu*: {payload.get('detected_at', '-')}",
        ]
        return "\n".join(baris)

    is_dq = jenis == "DATA_QUALITY_SURGE"
    judul = "Lonjakan data karantina" if is_dq else "Pipeline GAGAL"
    ikon = ":warning:" if is_dq else ":rotating_light:"

    baris = [
        f"{ikon} *{judul}* — {payload.get('severity', 'ERROR')}",
        f"• *DAG*: `{payload.get('dag_id', '-')}`",
    ]

    if is_dq:
        baris += [
            f"• *Layer*: {payload.get('layer', '-')}",
            f"• *Baris ditolak*: {payload.get('rejected_rows')} dari "
            f"{payload.get('total_rows')} ({payload.get('rejected_pct')}%)",
            f"• *Ambang*: {payload.get('threshold_pct')}%",
        ]
    else:
        baris += [
            f"• *Task*: `{payload.get('task_id', '-')}`",
            f"• *Alasan*: {payload.get('reason', '-')}",
        ]

    baris.append(f"• *Waktu*: {payload.get('detected_at', '-')}")

    if payload.get("log_url"):
        baris.append(f"• *Log*: <{payload['log_url']}|buka di Airflow>")
    if payload.get("exception"):
        baris.append(f"• *Error*: `{str(payload['exception'])[:400]}`")

    return "\n".join(baris)


def _send_webhook(payload: dict[str, Any]) -> None:
    """Kirim alert ke webhook bila dikonfigurasi (opsional).

    Slack Incoming Webhook, mis. ``https://hooks.slack.com/services/...``,
    menerima ``{"text": "..."}``. Webhook generik menerima payload JSON utuh.
    Keduanya dikirim dengan kunci yang tepat agar tidak ditolak.

    Sengaja best-effort: kegagalan mengirim alert tidak boleh menutupi
    kegagalan pipeline yang sebenarnya.
    """
    url = os.getenv("ALERT_WEBHOOK_URL")
    if not url:
        log.info("ALERT_WEBHOOK_URL tidak di-set — alert hanya dicatat di log.")
        return

    # Tiga pembatas sebelum benar-benar mengirim:
    #   should_send     -> alert dengan kunci sama tidak diulang dalam jendela dedup
    #   rate_limit_ok   -> menjaga batas keras Slack (±1 pesan/detik)
    #   burst_guard_ok  -> membatasi badai alert kegagalan task
    dedup_key = payload.get("dedup_key") or ""
    if not alert_throttle.should_send(dedup_key):
        return
    if not alert_throttle.rate_limit_ok():
        return

    # Pembatas badai hanya berlaku untuk alert per-task. Ringkasan DAG-run
    # dan peringatan watchdog sengaja dilewatkan: keduanya justru menjadi
    # sumber informasi utama ketika banyak task gagal bersamaan.
    jenis = payload.get("alert_type")
    if jenis == "PIPELINE_FAILURE" and not alert_throttle.burst_guard_ok():
        return

    if _is_slack(url):
        # Slack menolak payload tanpa key `text` (400 invalid_payload),
        # jadi kirim pesan teks. Payload utuh tetap tersedia di log.
        body = json.dumps({"text": _format_alert_text(payload)}).encode()
    else:
        body = json.dumps(payload).encode()

    try:
        import urllib.request

        req = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=10).close()
        log.info("Alert terkirim ke webhook (%s).", "slack" if _is_slack(url) else "generic")
    except Exception as exc:  # noqa: BLE001 - best effort
        # Sertakan isi respons bila ada: Slack memakai body untuk menjelaskan
        # alasan penolakan (mis. invalid_payload), sehingga tanpa ini pesan log
        # hanya berbunyi "HTTP Error 400" tanpa petunjuk apa pun.
        detail = ""
        read = getattr(exc, "read", None)
        if callable(read):
            try:
                detail = f" | respons: {read().decode(errors='replace')[:300]}"
            except Exception:  # noqa: BLE001
                detail = ""
        log.warning("Gagal mengirim alert ke webhook: %s%s", exc, detail)


def notify(context: dict[str, Any], *, severity: str = "ERROR", reason: str = "") -> dict[str, Any]:
    """Catat & (opsional) kirim alert kegagalan pipeline.

    Args:
        context: context dict dari Airflow (otomatis untuk callbacks).
        severity: ERROR | CRITICAL.
        reason: keterangan singkat penyebab (mis. 'dagrun_timeout').

    Returns:
        Payload alert (berguna untuk unit test).
    """
    ti = context.get("task_instance")
    dag = context.get("dag")
    dag_id = getattr(dag, "dag_id", None) or context.get("dag_id", "unknown")
    task_id = getattr(ti, "task_id", None)

    payload: dict[str, Any] = {
        "alert_type": "PIPELINE_FAILURE",
        "severity": severity,
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": context.get("run_id"),
        "logical_date": str(context.get("logical_date") or context.get("ds") or ""),
        "try_number": getattr(ti, "try_number", None),
        "reason": reason or "task_failed",
        "detected_at": _now_iso(),
        "log_url": getattr(ti, "log_url", None),
        # Kunci dedup per (dag, task). Bila satu task gagal berulang karena
        # retry, alert hanya dikirim sekali dalam jendela dedup.
        "dedup_key": f"failure:{dag_id}:{task_id}",
    }

    if context.get("exception"):
        payload["exception"] = str(context["exception"])[:500]

    log.error("PIPELINE FAILURE ALERT: %s", json.dumps(payload, ensure_ascii=False))
    _send_webhook(payload)
    return payload


def alert_dag_run_failed(dag_id: str, run_id: str) -> dict[str, Any]:
    """Kirim SATU alert ringkasan untuk seluruh kegagalan sebuah DAG-run.

    Kenapa perlu: alert per task bisa membanjiri Slack saat kegagalan
    sistemik — ingestion punya puluhan task paralel yang akan gagal
    bersamaan, sementara Slack membatasi ±1 pesan/detik sehingga sebagian
    pesan dibuang. Ringkasan per DAG-run memberi gambaran lengkap dalam
    satu pesan.

    Dipanggil lewat ``on_failure_callback`` Airflow tingkat DAG-run, bukan
    per task.

    Returns:
        Payload alert (berguna untuk pengujian).
    """
    gagal: list[str] = []
    total = 0
    try:
        with create_session() as session:
            tis = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.dag_id == dag_id,
                    TaskInstance.run_id == run_id,
                )
                .all()
            )
            total = len(tis)
            gagal = sorted(
                {
                    ti.task_id
                    for ti in tis
                    if ti.state in (State.FAILED, State.UPSTREAM_FAILED)
                }
            )
    except Exception as exc:  # noqa: BLE001 - jangan gagalkan callback
        log.warning("Tidak bisa membaca daftar task gagal: %s", exc)

    payload: dict[str, Any] = {
        "alert_type": "DAG_RUN_FAILURE",
        "severity": "ERROR",
        "dag_id": dag_id,
        "run_id": run_id,
        "failed_tasks": gagal,
        "failed_count": len(gagal),
        "total_tasks": total,
        "detected_at": _now_iso(),
        # Kunci dedup menyertakan run_id: tiap run boleh melapor sekali,
        # tetapi run yang sama tidak diulang-ulang.
        "dedup_key": f"dagrun:{dag_id}:{run_id}",
    }

    log.error("DAG RUN FAILURE ALERT: %s", json.dumps(payload, ensure_ascii=False))
    _send_webhook(payload)
    return payload


def on_dag_run_failure(context: dict[str, Any]) -> None:
    """Callback untuk ``on_failure_callback`` pada level DAG-run.

    Menghasilkan satu pesan ringkasan berisi daftar task yang gagal.
    """
    dag = context.get("dag")
    dag_id = getattr(dag, "dag_id", None) or context.get("dag_id", "unknown")
    run_id = context.get("run_id", "unknown")
    alert_dag_run_failed(dag_id, run_id)


def on_failure_alert(context: dict[str, Any]) -> None:
    """Callback standar untuk `default_args['on_failure_callback']`.

    Di Airflow 2.x, DAG yang melewati ``dagrun_timeout`` akan ditandai
    gagal, sehingga tetap masuk lewat callback ini.
    """
    notify(context, severity="ERROR", reason="task_failed")


def on_dagrun_timeout(context: dict[str, Any]) -> None:
    """Alert severity CRITICAL untuk pipeline yang MACET (bukan sekadar gagal).

    Catatan penting: Airflow 2.x **tidak** menyediakan parameter
    ``on_dagrun_timeout`` pada DAG, jadi fungsi ini TIDAK ter-wire otomatis.
    Timeout DAG-run tetap terdeteksi lewat ``on_failure_callback`` dengan
    ``reason='task_failed'``.

    Fungsi ini dipertahankan sebagai jalur eksplisit (mis. dipanggil dari
    watchdog/sensor, atau bila kelak memakai operator yang menerima callback
    timeout) agar severity CRITICAL tetap dapat dibedakan dari kegagalan biasa
    — dan supaya pembaca tidak menyimpulkan bahwa timeout tidak tertangani.
    """
    notify(context, severity="CRITICAL", reason="dagrun_timeout")


def on_sla_miss(dag, task_list, blocking_task_list, slm, services) -> None:
    """Callback SLA miss (kompatibel dengan signature Airflow)."""
    log.error(
        "SLA MISS ALERT: dag=%s task_list=%s", getattr(dag, "dag_id", "?"), task_list
    )


def alert_data_quality(
    *,
    layer: str,
    dag_id: str,
    rejected_pct: float,
    threshold_pct: float,
    total_rows: int,
    rejected_rows: int,
) -> dict[str, Any]:
    """Alert khusus lonjakan volume karantina.

    Dipicu bila proporsi data yang masuk karantina melewati threshold
    — sinyal masalah sistemik (skema sumber berubah / bug upstream), bukan
    sekadar noise yang boleh diabaikan.
    """
    payload = {
        "alert_type": "DATA_QUALITY_SURGE",
        "severity": "CRITICAL",
        "layer": layer,
        "dag_id": dag_id,
        "total_rows": total_rows,
        "rejected_rows": rejected_rows,
        "rejected_pct": round(rejected_pct, 4),
        "threshold_pct": threshold_pct,
        "detected_at": _now_iso(),
        # Dedup per layer: lonjakan pada sumber yang sama tidak perlu
        # diberitakan berulang kali dalam jendela dedup.
        "dedup_key": f"dq_surge:{layer}",
    }
    log.error("DATA QUALITY ALERT: %s", json.dumps(payload, ensure_ascii=False))
    _send_webhook(payload)
    return payload


def alert_streaming(
    *,
    check: str,
    title: str,
    detail: str,
    observed: Any = None,
    threshold: Any = None,
    severity: str = "WARNING",
    dedup_key: str | None = None,
) -> dict[str, Any]:
    """Alert dari watchdog streaming.

    Dipakai untuk kondisi yang membuat pipeline streaming **tidak lagi
    memberi data layak pakai**, misalnya data tidak diperbarui melewati
    ambang (consumer mati, laptop sleep) atau payload rusak beruntun.

    Ini tetap alert kegagalan pipeline, bukan notifikasi risiko operasional
    stasiun — risiko stasiun cukup ditampilkan sebagai indikator di dashboard.
    """
    payload: dict[str, Any] = {
        "alert_type": "STREAMING_WARNING",
        "severity": severity,
        "check": check,
        "title": title,
        "detail": detail,
        "observed": observed,
        "threshold": threshold,
        "detected_at": _now_iso(),
        "dag_id": "citibike_watchdog_streaming",
        # Cek yang sama tidak diberitakan berulang dalam jendela dedup;
        # tanpa ini, watchdog yang berjalan tiap 5 menit akan mengirim
        # alert yang sama terus-menerus selama gangguan berlangsung.
        "dedup_key": dedup_key or f"watchdog:{check}",
    }
    log.error("STREAMING WATCHDOG ALERT: %s", json.dumps(payload, ensure_ascii=False, default=str))
    _send_webhook(payload)
    return payload
