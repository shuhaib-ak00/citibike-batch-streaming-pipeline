#!/usr/bin/env bash
# ============================================================
# Buat bucket GCS datalake + pasang lifecycle policy.
#
# Prasyarat:
#   - gcloud CLI terpasang & sudah `gcloud auth login`
#   - `.env` sudah diisi (minimal GCP_PROJECT_ID, GCS_BUCKET_RAW)
#
# Jalankan dari mana saja:
#   bash infra/gcs/create_buckets.sh
#
# Struktur prefix yang disiapkan (GCS tidak punya folder asli,
# placeholder object dipakai agar prefix muncul di console):
#   raw/trips/                      trip history harian (batch)
#   raw/station_status/             snapshot GBFS (streaming)
#   raw/station_status/_rejected/   Dead Letter (payload gagal di-parse)
#   raw/station_information/        referensi stasiun
#   reference/                      sample/reference untuk replay
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set +a
fi

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID belum di-set. Isi .env dulu (cp .env.example .env).}"
: "${GCS_BUCKET_RAW:?GCS_BUCKET_RAW belum di-set di .env}"
# Sumber tunggalnya GCP_LOCATION di .env (dipakai bersama bucket & BigQuery)
GCS_LOCATION="${GCP_LOCATION:-asia-southeast2}"

# Git Bash/MSYS di Windows: wrapper 'gcloud' (shell script) kena konversi
# path sehingga gagal. Pakai varian .cmd bila tersedia.
if [[ "${OSTYPE:-}" == msys* || "${OSTYPE:-}" == cygwin* ]] && command -v gcloud.cmd >/dev/null 2>&1; then
  GCLOUD="gcloud.cmd"
else
  GCLOUD="gcloud"
fi

echo ">>> Project : ${GCP_PROJECT_ID}"
echo ">>> Bucket  : gs://${GCS_BUCKET_RAW}"
echo ">>> Lokasi  : ${GCS_LOCATION}"
echo

# ---------- 1. Buat bucket (idempoten) ----------
if "${GCLOUD}" storage buckets describe "gs://${GCS_BUCKET_RAW}" \
     --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  echo "[skip] Bucket gs://${GCS_BUCKET_RAW} sudah ada."
else
  echo "[create] Membuat bucket gs://${GCS_BUCKET_RAW}..."
  "${GCLOUD}" storage buckets create "gs://${GCS_BUCKET_RAW}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCS_LOCATION}" \
    --default-storage-class=STANDARD \
    --uniform-bucket-level-access
fi

# ---------- 2. Lifecycle policy ----------
echo "[lifecycle] Memasang lifecycle policy..."
"${GCLOUD}" storage buckets update "gs://${GCS_BUCKET_RAW}" \
  --lifecycle-file="${SCRIPT_DIR}/lifecycle_policy.json"

# ---------- 3. Placeholder prefix ----------
echo "[prefix] Membuat placeholder prefix..."
for prefix in \
  "raw/trips" \
  "raw/station_status" \
  "raw/station_status/_rejected" \
  "raw/station_information" \
  "reference"
do
  echo "prefix ${prefix}" | "${GCLOUD}" storage cp - \
    "gs://${GCS_BUCKET_RAW}/${prefix}/.keep" \
    --content-type=text/plain >/dev/null
done

# ---------- 4. Verifikasi ----------
echo
echo ">>> Isi bucket:"
"${GCLOUD}" storage ls --recursive "gs://${GCS_BUCKET_RAW}/" | head -20
echo
echo ">>> Selesai. Lifecycle policy aktif:"
"${GCLOUD}" storage buckets describe "gs://${GCS_BUCKET_RAW}" --format="value(lifecycle_config)"
