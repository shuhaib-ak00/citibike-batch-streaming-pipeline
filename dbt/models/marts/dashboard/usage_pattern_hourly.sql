-- ============================================================
-- usage_pattern_hourly — mart dashboard (chart 3: heatmap jam x hari)
--
-- Menunjukkan pola jam sibuk commuter: puncak pagi di area perkantoran, puncak
-- sore di area residensial.
--
-- Grain: 1 baris per (hari-dalam-minggu, jam).
-- ============================================================

WITH trips AS (
    SELECT
        day_of_week,
        start_hour,
        member_casual,
        duration_seconds
    FROM {{ ref('fct_trips') }}
),

aggregated AS (
    SELECT
        day_of_week,
        start_hour,
        COUNT(*)                                  AS total_trips,
        COUNTIF(member_casual = 'member')          AS member_trips,
        COUNTIF(member_casual = 'casual')          AS casual_trips,
        ROUND(AVG(duration_seconds) / 60, 2)       AS avg_duration_minutes
    FROM trips
    GROUP BY day_of_week, start_hour
)

SELECT
    a.day_of_week,
    d.day_name,
    d.is_weekend,
    a.start_hour,
    a.total_trips,
    a.member_trips,
    a.casual_trips,
    a.avg_duration_minutes,
    -- Pangsa terhadap total seluruh periode: membuat intensitas
    -- antar-sel heatmap bisa dibandingkan secara adil.
    ROUND(
        SAFE_DIVIDE(a.total_trips, SUM(a.total_trips) OVER ()) * 100,
        4
    )                                             AS share_of_total_pct
FROM aggregated AS a
-- dim_date dipakai hanya sebagai lookup nama hari (1=Senin .. 7=Minggu)
LEFT JOIN (
    SELECT DISTINCT day_of_week, day_name, is_weekend
    FROM {{ ref('dim_date') }}
) AS d
    ON a.day_of_week = d.day_of_week
