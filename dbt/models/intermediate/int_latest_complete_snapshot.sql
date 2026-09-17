-- ============================================================
-- int_latest_complete_snapshot — snapshot terakhir yang LENGKAP
--
-- Bila penulisan consumer terpotong di tengah snapshot, satu snapshot terpecah
-- menjadi beberapa bagian. Terbukti dari data nyata: 56 dari 118 snapshot tidak
-- lengkap, termasuk snapshot terakhir yang hanya berisi 1 baris.
--
-- Mart yang memakai "snapshot terakhir" secara buta akan menampilkan peta dan
-- daftar risiko yang nyaris kosong, padahal datanya ada.
--
-- "Lengkap" = memuat minimal 95% dari jumlah stasiun terbanyak pada snapshot
-- mana pun. Ambangnya relatif terhadap data, bukan angka tetap, sehingga tidak
-- perlu disesuaikan bila jumlah stasiun berubah.
--
-- Satu model tersendiri supaya hanya ada SATU definisi "snapshot terkini" di
-- seluruh project — beberapa mart membutuhkannya.
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
