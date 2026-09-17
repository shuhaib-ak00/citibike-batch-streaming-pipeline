"""DAG: ringkasan kegagalan per DAG-run.

Alert per task (``default_args.on_failure_callback``) memadai untuk kegagalan
tunggal, tetapi tidak untuk kegagalan sistemik: kunci dedup-nya berbeda per
task, sehingga Slack menerima puluhan pesan sekaligus.

Catatan pengembangan — JANGAN kembali ke ``on_failure_callback`` tingkat DAG.
Terbukti tidak dapat diandalkan di Airflow 2.9.3: ``DAG.fetch_callback``
membangun konteks dari ``tis[-1]``, dan untuk DAG ber-*dynamic task mapping*
pemanggilan itu melempar ``NotFullyPopulated`` sehingga callback tidak pernah
terkirim — justru pada DAG yang paling rawan gagal sistemik. Dua cacat lain
pendekatan itu: isi ``failed_tasks`` bisa kosong (task masih berstatus
``up_for_retry``, bukan ``failed``), dan callback diproses ulang setiap siklus
parsing DAG.

DAG ini membaca metadata Airflow langsung, sehingga baru melapor setelah
DAG-run benar-benar berstatus ``failed``.

Batas: mekanisme di dalam Airflow tidak bisa melaporkan Airflow sendiri yang
mati. Perlu pemantauan eksternal; keheningan bukan berarti sehat.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models import DagRun
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState

from common.alert_utils import alert_dag_run_failed

log = logging.getLogger(__name__)

# DAG-run gagal yang lebih tua dari ini tidak dilaporkan lagi. Nilainya harus
# lebih besar dari jeda jadwal, kalau tidak ada run yang sempat terlihat.
LOOKBACK_MINUTES = int(os.getenv("WATCHDOG_FAILED_RUN_LOOKBACK_MINUTES", "60"))

# Watchdog tidak melaporkan kegagalan miliknya sendiri; itu akan membuatnya
# melapor tanpa henti dan tidak ada gunanya.
DAG_ID = "citibike_watchdog_pipeline"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id=DAG_ID,
    description="Laporkan ringkasan setiap DAG-run yang gagal (satu pesan per run)",
    # Tiap 10 menit: cukup cepat untuk diketahui, tanpa membanjiri Slack.
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # Tanpa flag ini, DAG baru ter-pause pada clone segar dan tidak akan
    # pernah berjalan — kegagalan senyap yang justru ingin dicegah.
    is_paused_upon_creation=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    default_args=default_args,
    tags=["citibike", "monitoring", "alert", "batch"],
    doc_md=__doc__,
)
def citibike_watchdog_pipeline() -> None:

    @task
    def report_failed_runs() -> dict:
        """Kirim satu ringkasan untuk setiap DAG-run yang gagal baru-baru ini.

        Deduplikasi ditangani ``alert_dag_run_failed`` lewat kunci
        ``dagrun:<dag_id>:<run_id>``, sehingga run yang sama tidak dilaporkan
        dua kali meskipun watchdog berjalan berulang.
        """
        # `start_date` di Airflow bersifat timezone-aware, jadi batasnya juga
        # harus aware — membandingkan dengan datetime naif akan melempar
        # TypeError, bukan sekadar menghasilkan hasil yang keliru.
        batas = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)

        with create_session() as session:
            runs = (
                session.query(DagRun)
                .filter(
                    DagRun.state == DagRunState.FAILED,
                    DagRun.dag_id != DAG_ID,
                    DagRun.start_date.isnot(None),
                    DagRun.start_date >= batas,
                )
                .order_by(DagRun.start_date.desc())
                .all()
            )
            kandidat = [(r.dag_id, r.run_id) for r in runs]

        if not kandidat:
            log.info(
                "Tidak ada DAG-run gagal dalam %d menit terakhir.", LOOKBACK_MINUTES
            )
            return {"status": "OK", "failed_runs": 0, "dilaporkan": []}

        log.info("Ditemukan %d DAG-run gagal: %s", len(kandidat), kandidat)

        dilaporkan = []
        for dag_id, run_id in kandidat:
            # alert_dag_run_failed sudah menangani dedup dan rate limit;
            # pemanggilan berulang aman dan memang diharapkan.
            payload = alert_dag_run_failed(dag_id, run_id)
            dilaporkan.append(
                {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "failed_count": payload.get("failed_count", 0),
                    "failed_tasks": payload.get("failed_tasks", [])[:10],
                }
            )

        log.warning("Ringkasan kegagalan dilaporkan: %s", json.dumps(dilaporkan)[:800])
        return {
            "status": "FAILED_RUNS",
            "failed_runs": len(kandidat),
            "dilaporkan": dilaporkan,
        }

    @task
    def summarize(hasil: dict) -> dict:
        """Catat hasil dalam satu baris yang bisa dibaca cepat di UI Airflow."""
        if hasil.get("failed_runs"):
            log.warning("Watchdog pipeline: %d DAG-run gagal dilaporkan.", hasil["failed_runs"])
        else:
            log.info("Watchdog pipeline: tidak ada kegagalan DAG-run.")
        return hasil

    summarize(report_failed_runs())


citibike_watchdog_pipeline()
