# Folder ini menampung kredensial yang TIDAK boleh di-commit.
#
# Yang harus ada di sini (dibuat dari GCP Console > IAM > Service Accounts
# > Keys > Add key > Create new key > JSON):
#
#   service-account.json
#
# File itu di-mount ke container sebagai:
#   Airflow   : /opt/airflow/.gcp/service-account.json
#   Streaming : /opt/app/.gcp/service-account.json
#
# Service account butuh role:
#   - roles/bigquery.dataEditor    (tulis tabel raw & dataset dbt)
#   - roles/bigquery.jobUser      (menjalankan query)
#   - roles/storage.objectAdmin    (baca/tulis GCS datalake)
