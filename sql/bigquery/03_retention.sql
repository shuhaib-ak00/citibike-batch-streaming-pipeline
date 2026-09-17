-- ============================================================
-- Retention data streaming
--
-- Tabel `station_status` bersifat APPEND-ONLY: consumer menulis
-- terus-menerus dan tidak ada yang menghapus. Dengan ±2.500 stasiun per
-- snapshot dan interval 90 detik, tabel ini bertambah sekitar
-- **240 MB per hari** — cukup untuk menghabiskan kuota storage gratis
-- 10 GiB dalam hitungan minggu, dan membuat query "kondisi terkini"
-- makin mahal karena partisi hariannya makin besar.
--
-- Data yang dihapus di sini sudah tidak punya nilai analitis: mart
-- dashboard hanya memakai snapshot terakhir, sedangkan analisis tren
-- membutuhkan hitungan hari, bukan bulan.
--
-- ⚠️ Penghapusan memakai DML DELETE yang memindai kolom partisi, sehingga
-- hanya partisi lama yang dibaca. Jangan menjalankan query ini tanpa
-- filter tanggal — pemindaian seluruh tabel akan mahal.
-- ============================================================

-- Simpan 14 hari terakhir data status stasiun.
DELETE FROM `${GCP_PROJECT_ID}.${BQ_DATASET_RAW}.station_status`
WHERE _snapshot_date < DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY);

-- Dead Letter Queue: payload gagal tidak perlu disimpan lama, tetapi
-- cukup lama untuk memeriksa pola bila terjadi lonjakan.
DELETE FROM `${GCP_PROJECT_ID}.${BQ_DATASET_RAW}.station_status_dlq`
WHERE DATE(failed_at) < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

-- CATATAN: tabel karantina dbt (stg_trips_rejected,
-- stg_station_status_rejected) sengaja TIDAK dibersihkan di sini.
--
-- Keduanya bermaterialisasi VIEW, jadi tidak menyimpan data sendiri —
-- isinya selalu dihitung ulang dari tabel raw dan masa hidupnya otomatis
-- mengikuti raw. Perintah DELETE terhadap view akan gagal dengan error
-- "... is not allowed for this operation because it currently has type
-- VIEW", bukan sekadar tidak menghapus apa pun.
--
-- View juga memang pilihan yang tepat, bukan kekurangan: task
-- check_quarantine_surge menghitung rasio baris ditolak per eksekusi dbt,
-- sehingga ia butuh isi karantina yang mencerminkan data raw saat ini.
-- Bila tabelnya diakumulasi, pembilang rasio ikut memuat baris yang sudah
-- lama ditolak dan hasilnya salah.

-- Metrik DQ dipakai untuk melihat tren kualitas data; retensi lebih
-- panjang karena barisnya sedikit dan justru berguna sebagai riwayat.
DELETE FROM `${GCP_PROJECT_ID}.${BQ_DATASET_RAW}.dq_metrics`
WHERE run_date < DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);
