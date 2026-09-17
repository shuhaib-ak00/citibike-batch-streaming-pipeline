-- ============================================================
-- int_latest_complete_snapshot — penentu snapshot terakhir yang LENGKAP
--
-- Masalah yang diselesaikan
-- -------------------------
-- Consumer menulis data secara berkala (flush). Bila penulisan terjadi di
-- tengah sebuah snapshot, satu snapshot akan terpecah menjadi beberapa
-- bagian. Terbukti dari data nyata: dari 118 snapshot, 56 di antaranya
-- tidak lengkap — termasuk snapshot terakhir yang hanya berisi 1 baris.
--
-- Akibatnya, "snapshot terakhir" belum tentu berisi seluruh stasiun.
-- Mart yang memakai snapshot terakhir secara buta akan menampilkan peta
-- dan daftar risiko yang nyaris kosong, padahal datanya ada.
--
-- Cara menentukan "lengkap"
-- ------------------------
-- Sebuah snapshot dianggap lengkap bila memuat hampir seluruh stasiun yang
-- pernah terlihat, yaitu minimal 95% dari jumlah stasiun terbanyak pada
-- snapshot mana pun. Ambangnya relatif terhadap data, bukan angka tetap,
-- sehingga tidak perlu disesuaikan bila jumlah stasiun berubah.
--
-- Diletakkan di satu model tersendiri supaya hanya ada SATU definisi
-- "snapshot terkini" di seluruh project; beberapa mart membutuhkannya.
--
-- Grain: 1 baris berisi satu timestamp.
-- ============================================================

WITH snapshot_sizes AS (
    SELECT
        snapshot_timestamp,
        COUNT(DISTINCT gbfs_station_id) AS station_count
    FROM {{ ref('fct_station_status') }}
    GROUP BY snapshot_timestamp
),

threshold AS (
    SELECT CAST(MAX(station_count) * 0.95 AS INT64) AS min_complete_stations
    FROM snapshot_sizes
)

SELECT
    MAX(s.snapshot_timestamp) AS snapshot_timestamp,
    (SELECT min_complete_stations FROM threshold) AS min_complete_stations
FROM snapshot_sizes AS s
WHERE s.station_count >= (SELECT min_complete_stations FROM threshold)
