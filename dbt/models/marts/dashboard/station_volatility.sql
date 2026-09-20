-- ============================================================
-- station_volatility — mart dashboard (chart 8: stasiun paling fluktuatif)
--
-- Menjawab pertanyaan yang tidak bisa dijawab snapshot: "stasiun mana yang
-- paling sering berubah, dan berapa banyak sepeda yang bergerak di sana?"
--
-- Sumbernya arsip perubahan, bukan snapshot. Snapshot merekam kondisi tiap
-- polling (mayoritas baris identik dengan sebelumnya), sehingga menghitung
-- pergerakan dari sana berarti menghitung baris, bukan perubahan.
--
-- Catatan pengembangan:
--
-- - `changes_per_hour` WAJIB ada, bukan sekadar `change_count`. Stasiun yang
--   muncul belakangan punya rentang observasi lebih pendek, sehingga hitungan
--   mentah tampak lebih kecil padahal belum tentu lebih tenang. Laju per jam
--   membuat semua stasiun sebanding.
--
-- - Label volatilitas memakai PERINGKAT PERSENTIL, bukan ambang tetap. Ambang
--   seperti ">20 perubahan/jam" akan salah begitu kecepatan polling berubah;
--   potongan persentil tetap bermakna karena selalu relatif terhadap data.
--
-- - `total_moved` = jumlah sepeda+dock yang BERGERAK, bukan selisih bersih.
--   Stasiun yang 10 -> 5 -> 10 dihitung 10, karena 10 sepeda memang berpindah.
--   Selisih bersih mencatat 0 dan menyembunyikan aktivitasnya.
--
-- - `op='c'` punya delta NULL (tidak ada keadaan sebelumnya), jadi dihitung 0.
--   Barisnya tetap dihitung sebagai perubahan, karena kemunculan stasiun
--   memang perubahan.
--
-- - View, TIDAK seperti tujuh mart lain di folder ini. Sumbernya arsip
--   perubahan, yang bertambah setiap DAG streaming berjalan (tiap jam),
--   sementara model ini tidak ikut dibangun DAG itu. Sebagai table ia akan
--   basi sampai 24 jam; sebagai view ia selalu mengikuti arsip tanpa perlu
--   dijadwalkan sendiri.
--
--   Perhitungannya juga murah: agregasi 73 ribu baris tanpa join, jauh lebih
--   ringan daripada mart lain yang memindai fct_trips (5,9 juta baris).
--
-- Grain: 1 baris per stasiun.
-- ============================================================

{{ config(materialized='view') }}

WITH perubahan AS (
    SELECT * FROM {{ ref('int_station_status_changes') }}
),

ringkas AS (
    SELECT
        gbfs_station_id,
        ANY_VALUE(station_name)                       AS station_name,
        ANY_VALUE(legacy_station_id)                  AS legacy_station_id,

        COUNT(*)                                      AS change_count,
        COUNTIF(op = 'u')                             AS update_count,

        -- ABS dipakai karena yang diukur adalah pergerakan, bukan arah.
        -- COALESCE 0 untuk op='c' yang deltanya memang tidak ada.
        SUM(ABS(COALESCE(delta_bikes, 0)))            AS bikes_moved,
        SUM(ABS(COALESCE(delta_docks, 0)))            AS docks_moved,

        MIN(valid_from)                               AS first_change_at,
        MAX(valid_from)                               AS last_change_at,

        -- Rentang observasi per stasiun; dipakai menormalkan laju perubahan.
        TIMESTAMP_DIFF(MAX(valid_from), MIN(valid_from), MINUTE) / 60.0
                                                      AS span_hours
    FROM perubahan
    GROUP BY gbfs_station_id
)

SELECT
    gbfs_station_id,
    legacy_station_id,
    -- 246 stasiun (10%) tidak punya station_name karena tidak ada di
    -- dim_station (is_unknown_station). Dibiarkan NULL, chart akan menampilkan
    -- label kosong dan barisnya tidak bisa dikenali. Dipakai id pendek sebagai
    -- ganti supaya barisnya tetap terbaca sekaligus menandai bahwa stasiun itu
    -- belum terpetakan ke dimensi.
    COALESCE(
        station_name,
        CONCAT('Stasiun tak terpetakan (', SUBSTR(gbfs_station_id, 1, 8), ')')
    )                                                 AS station_name,

    change_count,
    update_count,
    bikes_moved,
    docks_moved,
    bikes_moved + docks_moved                         AS total_moved,

    first_change_at,
    last_change_at,
    ROUND(span_hours, 2)                              AS span_hours,

    -- Laju perubahan. SAFE_DIVIDE dipakai karena stasiun yang baru muncul
    -- bisa punya rentang 0 menit (satu perubahan saja).
    ROUND(SAFE_DIVIDE(change_count, span_hours), 2)   AS changes_per_hour,

    -- Label dari posisi relatif, bukan ambang tetap — lihat catatan di atas.
    CASE
        WHEN PERCENT_RANK() OVER (
                 ORDER BY COALESCE(SAFE_DIVIDE(change_count, span_hours), 0)
             ) >= 0.90 THEN 'sangat_fluktuatif'
        WHEN PERCENT_RANK() OVER (
                 ORDER BY COALESCE(SAFE_DIVIDE(change_count, span_hours), 0)
             ) >= 0.70 THEN 'fluktuatif'
        ELSE 'tenang'
    END                                               AS volatility_label,

    -- Peringkat untuk pengurutan chart.
    RANK() OVER (ORDER BY bikes_moved + docks_moved DESC)
                                                      AS movement_rank
FROM ringkas
