"""Satu-satunya tempat yang membahas CARA dbt dieksekusi.

Sekarang  : BashOperator  -> dbt terpasang di image Airflow.
Nanti     : DockerOperator -> cukup ganti isi `dbt_task()`; seluruh DAG
             dan fungsi `dbt_command()` TIDAK perlu diubah.

Prinsip: DAG mendefinisikan **perintah apa** yang dijalankan, bukan
**operator apa** yang mengeksekusinya.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.operators.bash import BashOperator

from common.config import DBT_PROFILES_DIR, DBT_PROJECT_DIR

# Catatan: lokasi paket dbt (dbt_utils) TIDAK diatur dari sini, melainkan
# lewat `packages-install-path` di dbt/dbt_project.yml. Sebabnya, `dbt deps`
# maupun `dbt run` sama-sama TIDAK menerima flag `--packages-install-path`
# di dbt-core 1.8.7 — keduanya menolak dengan "No such option". Hanya
# `dbt_project.yml` yang didukung.

# Cek awal yang ramah: profiles.yml di-commit (tanpa secret), tapi bila
# seseorang menghapusnya, pesan errornya harus jelas — bukan traceback dbt.
_PREFLIGHT = (
    f'if [ ! -f "{DBT_PROFILES_DIR}/profiles.yml" ]; then '
    f'echo "ERROR: {DBT_PROFILES_DIR}/profiles.yml tidak ditemukan. '
    f'Jalankan: cp {DBT_PROFILES_DIR}/profiles.yml.example '
    f'{DBT_PROFILES_DIR}/profiles.yml"; exit 1; fi'
)


def dbt_command(
    subcommand: str,
    select: str = "",
    *,
    full_refresh: bool = False,
    extra: str = "",
    target_path: str = "",
) -> str:
    """Bangun perintah dbt — bagian yang stabil & tidak berubah saat migrasi.

    Args:
        target_path: Bila diisi, dbt menulis artefaknya (`manifest.json`,
            `run_results.json`) ke direktori itu alih-alih `target/` bawaan.
            Diperlukan karena dua DAG kini menjalankan dbt secara bersamaan
            (batch harian dan streaming tiap jam): tanpa direktori terpisah,
            `dbt test` dapat membaca `manifest.json` yang sedang ditulis
            proses lain dan gagal dengan error parse yang membingungkan.
            Dampak ke warehouse tetap aman — `CREATE OR REPLACE` bersifat
            atomik, jadi pembaca tidak pernah melihat versi setengah jadi.
    """
    parts = [
        f"dbt {subcommand}",
        f"--project-dir {DBT_PROJECT_DIR}",
        f"--profiles-dir {DBT_PROFILES_DIR}",
        "--no-use-colors",
    ]
    # `dbt deps` TIDAK menerima `--target-path` — dbt-core 1.8.7 menolaknya
    # dengan "Error: No such option: --target-path (Possible options:
    # --log-path, --target)". Perintah itu hanya menyiapkan paket, jadi
    # memang tidak menghasilkan artefak yang perlu dipisahkan.
    #
    # Dikunci di sini, bukan di pemanggil, supaya kesalahan yang sama tidak
    # terulang saat ada DAG baru yang ikut memakai target_path.
    if target_path and subcommand != "deps":
        parts.append(f"--target-path {target_path}")
    if select:
        parts.append(f"--select {select}")
    if full_refresh:
        parts.append("--full-refresh")
    if extra:
        parts.append(extra)
    return " ".join(parts)


def dbt_task(
    task_id: str,
    subcommand: str,
    select: str = "",
    *,
    full_refresh: bool = False,
    extra: str = "",
    target_path: str = "",
    retries: int = 0,
    retry_delay_minutes: int = 3,
    **kwargs,
) -> BashOperator:
    """Bangun task dbt. Bagian inilah yang berubah saat migrasi ke DockerOperator.

    Versi DockerOperator nanti (tidak perlu menyentuh `dbt_command`):

        return DockerOperator(
            task_id=task_id,
            image="citibike-dbt:latest",
            command=dbt_command(subcommand, select, full_refresh=full_refresh),
            ...
        )
    """
    command = dbt_command(
        subcommand, select,
        full_refresh=full_refresh, extra=extra, target_path=target_path,
    )
    return BashOperator(
        task_id=task_id,
        bash_command=f'cd "{DBT_PROJECT_DIR}" && {_PREFLIGHT} && {command}',
        retries=retries,
        retry_delay=timedelta(minutes=retry_delay_minutes),
        **kwargs,
    )
