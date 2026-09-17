-- ============================================================
-- int_trips_dq_summary — metrik kualitas data trip
--
-- Menyediakan angka yang dipakai untuk:
--   1. Audit: berapa baris valid, berapa dikarantina, berapa duplikat.
--   2. Memicu alert lonjakan karantina bila proporsinya melewati ambang.
--
-- Grain: 1 baris per kombinasi (source_file, rejection_reason).
-- Baris dengan rejection_reason NULL merepresentasikan baris VALID.
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'trips') }}
),

-- Replikasi definisi valid dari macro yang sama dengan stg_trips,
-- supaya angka metrik tidak mungkin menyimpang dari isi model.
classified AS (
    SELECT
        _source_file,
        NULLIF(TRIM(ride_id), '')                       AS ride_id_raw,
        ARRAY_TO_STRING(
            [
                IF(NULLIF(TRIM(ride_id), '') IS NULL, 'missing_ride_id', NULL),
                IF(started_at IS NULL, 'missing_started_at', NULL),
                IF(ended_at IS NULL, 'missing_ended_at', NULL),
                IF(started_at IS NOT NULL AND ended_at IS NOT NULL
                   AND TIMESTAMP_DIFF(ended_at, started_at, SECOND) < 0,
                   'negative_duration', NULL),
                IF(started_at IS NOT NULL AND ended_at IS NOT NULL
                   AND TIMESTAMP_DIFF(ended_at, started_at, SECOND) > {{ var('max_trip_duration_seconds') }},
                   'duration_extreme', NULL),
                IF(NULLIF(REGEXP_REPLACE(TRIM(COALESCE(start_station_id, '')), r'_$', ''), '') IS NOT NULL
                   AND NOT REGEXP_CONTAINS(NULLIF(REGEXP_REPLACE(TRIM(COALESCE(start_station_id, '')), r'_$', ''), ''),
                                           r'^([0-9]+(\.[0-9]+)?|(SYS|JC|HB)[0-9]+)$'),
                   'invalid_start_station_id', NULL),
                IF(NULLIF(REGEXP_REPLACE(TRIM(COALESCE(end_station_id, '')), r'_$', ''), '') IS NOT NULL
                   AND NOT REGEXP_CONTAINS(NULLIF(REGEXP_REPLACE(TRIM(COALESCE(end_station_id, '')), r'_$', ''), ''),
                                           r'^([0-9]+(\.[0-9]+)?|(SYS|JC|HB)[0-9]+)$'),
                   'invalid_end_station_id', NULL),
                IF(NULLIF(REGEXP_REPLACE(TRIM(COALESCE(start_station_id, '')), r'_$', ''), '') IS NULL
                   AND (start_lat IS NULL OR start_lng IS NULL),
                   'missing_start_location', NULL),
                IF(NULLIF(REGEXP_REPLACE(TRIM(COALESCE(end_station_id, '')), r'_$', ''), '') IS NULL
                   AND (end_lat IS NULL OR end_lng IS NULL),
                   'missing_end_location', NULL)
            ],
            ','
        ) AS rejection_reason_raw
    FROM source
),

-- Tandai duplikat: baris ke-2 dst untuk ride_id yang sama
with_dup_flag AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ride_id_raw
            ORDER BY rejection_reason_raw
        ) AS dup_rank
    FROM classified
)

SELECT
    _source_file,
    NULLIF(rejection_reason_raw, '')                     AS rejection_reason,
    COUNT(*)                                             AS row_count,
    COUNTIF(rejection_reason_raw = '' AND dup_rank = 1)   AS valid_rows,
    COUNTIF(rejection_reason_raw != '')                   AS rejected_rows,
    COUNTIF(rejection_reason_raw = '' AND dup_rank > 1)   AS duplicate_rows,
    ROUND(
        SAFE_DIVIDE(COUNTIF(rejection_reason_raw != ''), COUNT(*)) * 100,
        4
    )                                                    AS rejected_pct,
    CURRENT_TIMESTAMP()                                  AS measured_at
FROM with_dup_flag
GROUP BY _source_file, rejection_reason
