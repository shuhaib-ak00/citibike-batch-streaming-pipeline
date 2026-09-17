#!/usr/bin/env bash
# ============================================================
# Buat service account untuk pipeline + unduh kunci JSON.
#
# Kenapa script ini ada: menghindari langkah manual di GCP Console
# dan membuat setup bisa direproduksi ulang.
#
# Prasyarat:
#   - gcloud CLI terpasang & sudah `gcloud auth login`
#   - Akun kamu punya role Owner/Editor di project
#
# Jalankan:
#   bash infra/gcp/create_service_account.sh
#
# Output: secrets/service-account.json (di-ignore git)
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

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID belum di-set. Isi .env dulu.}"

# Git Bash/MSYS di Windows: wrapper 'gcloud' (shell script) kena konversi
# path sehingga gagal. Pakai varian .cmd bila tersedia.
if [[ "${OSTYPE:-}" == msys* || "${OSTYPE:-}" == cygwin* ]] && command -v gcloud.cmd >/dev/null 2>&1; then
  GCLOUD="gcloud.cmd"
else
  GCLOUD="gcloud"
fi

SA_NAME="${SA_NAME:-citibike-pipeline}"
SA_DISPLAY_NAME="${SA_DISPLAY_NAME:-Citi-Bike-Pipeline}"
SA_EMAIL="${SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
KEY_PATH="${ROOT_DIR}/secrets/service-account.json"

echo ">>> Project : ${GCP_PROJECT_ID}"
echo ">>> SA      : ${SA_EMAIL}"
echo

# CATATAN Windows/Git Bash: nilai argumen TIDAK boleh mengandung spasi.
# Wrapper `.cmd` milik Cloud SDK memecah argumen ber-spasi sehingga gcloud
# gagal dengan error "'C:\...' is not recognized". Karena itu display-name
# memakai tanda hubung, bukan spasi.

# ---------- 1. Buat service account ----------
if "${GCLOUD}" iam service-accounts describe "${SA_EMAIL}" \
     --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  echo "[skip] Service account sudah ada."
else
  echo "[create] Membuat service account..."
  "${GCLOUD}" iam service-accounts create "${SA_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --display-name="${SA_DISPLAY_NAME}"
fi

# ---------- 2. Beri role (minimal sesuai kebutuhan) ----------
echo "[iam] Memasang role..."
for role in \
  "roles/bigquery.dataEditor" \
  "roles/bigquery.jobUser" \
  "roles/storage.objectAdmin"
do
  echo "      - ${role}"
  "${GCLOUD}" projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${role}" \
    --condition=None \
    --quiet >/dev/null
done

# ---------- 3. Unduh kunci JSON ----------
mkdir -p "${ROOT_DIR}/secrets"
if [[ -f "${KEY_PATH}" ]]; then
  echo "[skip] ${KEY_PATH} sudah ada — tidak ditimpa."
  echo "       Hapus file itu dulu bila ingin membuat kunci baru."
else
  echo "[key] Membuat kunci JSON -> ${KEY_PATH}"
  gcloud iam service-accounts keys create "${KEY_PATH}" \
    --iam-account="${SA_EMAIL}" \
    --project="${GCP_PROJECT_ID}"
fi

echo
echo ">>> Selesai."
echo ">>> Pastikan .env berisi:"
echo "      GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/.gcp/service-account.json"
echo ">>> Untuk menjalankan dbt dari host, override ke:"
echo "      ${KEY_PATH}"
