-- ============================================================
-- fct_trips — fact table trip (transaction fact)
--
-- Grain: 1 baris per trip.
-- Partition: harian pada trip_date — memangkas bytes scanned saat
--   dashboard memfilter rentang tanggal.
-- Cluster: start_station_id + member_casual — kolom yang paling
--   sering dipakai untuk filter/agregasi di dashboard.
-- ============================================================

{{ config(
    materialized='table',
    partition_by={'field': 'trip_date', 'data_type': 'date'},
    cluster_by=['start_station_id', 'member_casual']
) }}

WITH trips AS (
    SELECT * FROM {{ ref('stg_trips') }}
)

SELECT
    -- kunci surrogate
    {{ dbt_utils.generate_surrogate_key(['ride_id']) }}               AS trip_key,

    -- Bila station_id tidak teridentifikasi, kunci dibiarkan NULL —
    -- bukan hash dari nilai kosong. Ini menjaga makna test
    -- `relationships` (test itu melewati NULL, tapi gagal bila ada
    -- kunci yang tidak ada di dimensi).
    CASE WHEN start_station_id IS NOT NULL
         THEN {{ dbt_utils.generate_surrogate_key(['start_station_id']) }}
    END                                                                AS start_station_key,
    CASE WHEN end_station_id IS NOT NULL
         THEN {{ dbt_utils.generate_surrogate_key(['end_station_id']) }}
    END                                                                AS end_station_key,

    CASE WHEN member_casual IS NOT NULL
         THEN {{ dbt_utils.generate_surrogate_key(['member_casual']) }}
    END                                                                AS rider_type_key,

    CAST(FORMAT_DATE('%Y%m%d', trip_date) AS INT64)                    AS start_date_key,

    -- natural / degenerate
    ride_id,
    rideable_type,
    member_casual,

    -- waktu
    started_at,
    ended_at,
    trip_date,
    EXTRACT(HOUR FROM started_at)                                      AS start_hour,
    -- ISO: 1 = Senin ... 7 = Minggu (lihat macro iso_day_of_week)
    {{ iso_day_of_week('started_at') }}                                AS day_of_week,
    duration_seconds,

    -- stasiun
    start_station_id,
    end_station_id,
    start_station_name,
    end_station_name,
    start_lat,
    start_lng,
    end_lat,
    end_lng,

    -- metrik turunan: jarak garis lurus (haversine via ST_DISTANCE).
    -- NULL bila salah satu koordinat tidak ada -> tetap jujur, tidak
    -- diisi 0 yang bisa menyesatkan rata-rata.
    ROUND(
        ST_DISTANCE(
            ST_GEOGPOINT(start_lng, start_lat),
            ST_GEOGPOINT(end_lng, end_lat)
        ) / 1000,
        3
    )                                                                   AS distance_km,

    -- jejak audit
    _source_file,
    _ingested_at
FROM trips
