-- ============================================================
-- Retention tabel karantina / DLQ (jalur manual)
--
-- Jalur utama adalah DAG citibike_retention_cleanup (mingguan). Skrip ini
-- disimpan untuk keperluan insiden, saat pembersihan perlu dijalankan tanpa
-- menunggu jadwal.
--
-- Retention default: 30 hari.
-- ============================================================

-- Hapus DLQ lebih tua dari 30 hari
DELETE FROM `${GCP_PROJECT_ID}.${BQ_DATASET_RAW}.station_status_dlq`
WHERE failed_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY);

-- CATATAN PENTING: tabel karantina dbt TIDAK dibersihkan di sini.
--
-- `stg_trips_rejected` dan `stg_station_status_rejected` bermaterialisasi
-- VIEW, sehingga DELETE terhadapnya gagal dengan error "... is not allowed
-- for this operation because it currently has type VIEW".
--
-- View memang pilihan yang tepat: task check_quarantine_surge menghitung
-- rasio baris ditolak per eksekusi dbt, jadi isinya harus selalu
-- mencerminkan data raw saat ini — bukan akumulasi historis. Karena view
-- tidak menyimpan data sendiri, masa hidupnya otomatis mengikuti tabel raw
-- dan retensi tersendiri memang tidak diperlukan.

-- ============================================================
-- Monitoring: proporsi karantina 7 hari terakhir
-- (dipakai runbook untuk verifikasi manual; alert otomatis ada di DAG)
-- ============================================================
-- SELECT
--   run_date,
--   layer,
--   total_rows,
--   rejected_rows,
--   ROUND(rejected_pct, 2) AS rejected_pct
-- FROM `${GCP_PROJECT_ID}.${BQ_DATASET_RAW}.dq_metrics`
-- WHERE run_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
-- ORDER BY run_date DESC, layer;
