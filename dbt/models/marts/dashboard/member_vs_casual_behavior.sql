-- ============================================================
-- member_vs_casual_behavior — mart dashboard (chart 4: member vs casual)
--
-- Membandingkan profil dua segmen pengguna: volume, durasi, jarak, jenis sepeda,
-- dan pola waktu (weekday vs weekend).
--
-- Grain: 1 baris per rider_type.
-- ============================================================

WITH trips AS (
    SELECT
        f.member_casual,
        f.duration_seconds,
        f.distance_km,
        f.rideable_type,
        f.start_hour,
        d.is_weekend
    FROM {{ ref('fct_trips') }} AS f
    LEFT JOIN {{ ref('dim_date') }} AS d
        ON f.trip_date = d.date_day
),

aggregated AS (
    SELECT
        member_casual,
        COUNT(*)                                        AS total_trips,
        ROUND(AVG(duration_seconds) / 60, 2)             AS avg_duration_minutes,
        ROUND(APPROX_QUANTILES(duration_seconds, 100)[OFFSET(50)] / 60, 2)
                                                         AS median_duration_minutes,
        ROUND(AVG(distance_km), 3)                       AS avg_distance_km,
        COUNTIF(rideable_type = 'electric_bike')         AS ebike_trips,
        COUNTIF(is_weekend)                              AS weekend_trips,
        COUNTIF(NOT COALESCE(is_weekend, FALSE))         AS weekday_trips,
        -- jam sibuk pagi (07-09) vs sore (17-19)
        COUNTIF(start_hour BETWEEN 7 AND 9)              AS morning_peak_trips,
        COUNTIF(start_hour BETWEEN 17 AND 19)            AS evening_peak_trips
    FROM trips
    GROUP BY member_casual
)

SELECT
    r.rider_type_key,
    a.member_casual,
    r.description,
    a.total_trips,
    ROUND(SAFE_DIVIDE(a.total_trips, SUM(a.total_trips) OVER ()) * 100, 2)
                                                         AS share_of_total_pct,
    a.avg_duration_minutes,
    a.median_duration_minutes,
    a.avg_distance_km,
    a.ebike_trips,
    ROUND(SAFE_DIVIDE(a.ebike_trips, a.total_trips) * 100, 2)
                                                         AS ebike_share_pct,
    a.weekend_trips,
    a.weekday_trips,
    ROUND(SAFE_DIVIDE(a.weekend_trips, a.total_trips) * 100, 2)
                                                         AS weekend_share_pct,
    a.morning_peak_trips,
    a.evening_peak_trips
FROM aggregated AS a
LEFT JOIN {{ ref('dim_rider_type') }} AS r
    ON a.member_casual = r.member_casual
