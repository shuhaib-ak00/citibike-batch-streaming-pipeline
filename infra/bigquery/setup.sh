#!/usr/bin/env bash
# ============================================================
# Siapkan BigQuery: dataset + tabel raw layer.
#
# Prasyarat:
#   - bq CLI (bagian Google Cloud SDK) terpasang
#   - `.env` sudah diisi
#   - Bucket GCS sudah dibuat (infra/gcs/create_buckets.sh)
#
# Jalankan:
#   bash infra/bigquery/setup.sh
#
# Script ini idempoten: aman dijalankan berulang.
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DDL_FILE="${ROOT_DIR}/sql/bigquery/01_raw_tables_ddl.sql"

if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set +a
fi

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID belum di-set. Isi .env dulu.}"
: "${BQ_DATASET_RAW:?BQ_DATASET_RAW belum di-set di .env}"
BQ_LOCATION="${GCP_LOCATION:-asia-southeast2}"

# Git Bash/MSYS di Windows: wrapper 'bq' (shell script) kena konversi path
# sehingga gagal. Pakai varian .cmd bila tersedia.
if [[ "${OSTYPE:-}" == msys* || "${OSTYPE:-}" == cygwin* ]] && command -v bq.cmd >/dev/null 2>&1; then
  BQ="bq.cmd"
else
  BQ="bq"
fi

echo ">>> Project     : ${GCP_PROJECT_ID}"
echo ">>> Dataset raw : ${BQ_DATASET_RAW}"
echo ">>> Lokasi      : ${BQ_LOCATION}"
echo

# ---------- 1. Buat dataset raw (idempoten) ----------
if "${BQ}" --project_id="${GCP_PROJECT_ID}" show --dataset "${GCP_PROJECT_ID}:${BQ_DATASET_RAW}" >/dev/null 2>&1; then
  echo "[skip] Dataset ${BQ_DATASET_RAW} sudah ada."
else
  echo "[create] Membuat dataset ${BQ_DATASET_RAW}..."
  "${BQ}" --project_id="${GCP_PROJECT_ID}" mk \
    --dataset \
    --location="${BQ_LOCATION}" \
    --description="Raw-layer-Citi-Bike-data-sumber-apa-adanya" \
    "${GCP_PROJECT_ID}:${BQ_DATASET_RAW}"
fi

# ---------- 2. Render DDL (substitusi variabel .env) ----------
if ! [[ -f "${DDL_FILE}" ]]; then
  echo "ERROR: file DDL tidak ditemukan: ${DDL_FILE}" >&2
  exit 1
fi

echo "[ddl] Menjalankan DDL dari ${DDL_FILE}..."
if command -v envsubst >/dev/null 2>&1; then
  DDL_RENDERED="$(GCP_PROJECT_ID="${GCP_PROJECT_ID}" \
                  BQ_DATASET_RAW="${BQ_DATASET_RAW}" \
                  envsubst < "${DDL_FILE}")"
else
  echo "[ddl] envsubst tidak ada — memakai sed sebagai fallback."
  DDL_RENDERED="$(sed \
    -e "s|\${GCP_PROJECT_ID}|${GCP_PROJECT_ID}|g" \
    -e "s|\${BQ_DATASET_RAW}|${BQ_DATASET_RAW}|g" \
    "${DDL_FILE}")"
fi

printf '%s' "${DDL_RENDERED}" | "${BQ}" --project_id="${GCP_PROJECT_ID}" query \
  --use_legacy_sql=false \
  --location="${BQ_LOCATION}"

# ---------- 3. Verifikasi ----------
echo
echo ">>> Tabel di ${BQ_DATASET_RAW}:"
"${BQ}" --project_id="${GCP_PROJECT_ID}" ls --format=prettyjson "${BQ_DATASET_RAW}" \
  | grep -E '"tableReference"|"tableId"' || true
echo
echo ">>> Selesai. Dataset dbt (staging/intermediate/marts) dibuat otomatis oleh dbt saat run pertama."
