-- ============================================================
-- dim_date — dimensi tanggal (date spine)
--
-- Dimensi bersama untuk kedua fact table:
--   fct_trips           (batch)     -> rentang Des 2025 - Mar 2026
--   fct_station_status  (streaming) -> rentang hari berjalan
--
-- Rentangnya diambil dari GABUNGAN kedua sumber. Bila hanya dari trip, fact
-- streaming akan punya tanggal di luar dimensi sehingga foreign key-nya gagal.
-- Tidak ada tanggal kosong dalam rentang karena array tanggal dibangkitkan
-- berurutan.
--
-- Model ini dibangun DAG BATCH (harian), sedangkan rantai streaming berjalan
-- tiap jam. Karena itu batas atasnya diberi buffer ke depan — lihat komentar
-- pada GREATEST di bawah.
-- ============================================================

WITH bounds AS (
    SELECT
        LEAST(
            COALESCE((SELECT MIN(trip_date) FROM {{ ref('stg_trips') }}), DATE '2026-01-01'),
            COALESCE((SELECT MIN(_snapshot_date) FROM {{ ref('stg_station_status') }}), DATE '2026-01-01')
        ) AS min_day,
        GREATEST(
            COALESCE((SELECT MAX(trip_date) FROM {{ ref('stg_trips') }}), DATE '2026-03-31'),
            COALESCE((SELECT MAX(_snapshot_date) FROM {{ ref('stg_station_status') }}), DATE '2026-03-31'),
            -- Buffer ke depan, untuk menutup celah tengah malam.
            --
            -- Dimensi ini dibangun DAG batch (harian), sedangkan rantai
            -- streaming diperbarui tiap jam. Bila streaming berjalan melewati
            -- tengah malam sebelum batch selesai, snapshot hari baru belum
            -- punya baris di sini dan foreign key fct_station_status ->
            -- dim_date akan gagal. Buffer ini membuat dimensi selalu siap
            -- lebih dulu.
            DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY)
        ) AS max_day
),

spine AS (
    SELECT day AS date_day
    FROM bounds, UNNEST(GENERATE_DATE_ARRAY(bounds.min_day, bounds.max_day, INTERVAL 1 DAY)) AS day
)

SELECT
    CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64)      AS date_key,
    date_day,
    EXTRACT(YEAR   FROM date_day)                       AS year,
    EXTRACT(QUARTER FROM date_day)                      AS quarter,
    EXTRACT(MONTH  FROM date_day)                       AS month,
    FORMAT_DATE('%B', date_day)                         AS month_name,
    EXTRACT(DAY    FROM date_day)                       AS day_of_month,
    EXTRACT(ISOYEAR FROM date_day)                      AS iso_year,
    EXTRACT(ISOWEEK FROM date_day)                      AS iso_week,
    -- ISO: 1 = Senin ... 7 = Minggu (lihat macro iso_day_of_week)
    {{ iso_day_of_week('date_day') }}                   AS day_of_week,
    FORMAT_DATE('%A', date_day)                         AS day_name,
    {{ iso_day_of_week('date_day') }} IN (6, 7)          AS is_weekend
FROM spine
