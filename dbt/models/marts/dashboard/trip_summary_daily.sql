-- ============================================================
-- trip_summary_daily — mart dashboard #1
-- Chart: line "total trips per hari" (tren demand harian).
--
-- Grain: 1 baris per tanggal.
-- ============================================================

WITH trips AS (
    SELECT * FROM {{ ref('fct_trips') }}
),

daily AS (
    SELECT
        trip_date,
        COUNT(*)                                              AS total_trips,
        COUNTIF(member_casual = 'member')                      AS member_trips,
        COUNTIF(member_casual = 'casual')                      AS casual_trips,
        COUNTIF(rideable_type = 'electric_bike')               AS ebike_trips,
        COUNTIF(rideable_type = 'classic_bike')                AS classic_trips,
        ROUND(AVG(duration_seconds) / 60, 2)                   AS avg_duration_minutes,
        ROUND(SUM(distance_km), 1)                             AS total_distance_km,
        COUNT(DISTINCT start_station_id)                       AS active_start_stations
    FROM trips
    GROUP BY trip_date
)

SELECT
    -- t = daily (tabel utama), d = dim_date (pengayaan nama hari)
    t.trip_date                                               AS date_day,
    d.date_key,
    d.day_name,
    d.is_weekend,
    t.total_trips,
    t.member_trips,
    t.casual_trips,
    t.ebike_trips,
    t.classic_trips,
    t.avg_duration_minutes,
    t.total_distance_km,
    t.active_start_stations,
    -- Pangsa member — dipakai untuk membaca tren komposisi pengguna
    ROUND(SAFE_DIVIDE(t.member_trips, t.total_trips) * 100, 2) AS member_share_pct
FROM daily AS t
LEFT JOIN {{ ref('dim_date') }} AS d
    ON t.trip_date = d.date_day
