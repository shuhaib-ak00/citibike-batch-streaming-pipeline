-- ============================================================
-- stg_trips_rejected — tabel karantina trip
--
-- Prinsip "never silently drop data": baris yang gagal validasi bisnis
-- tetap disimpan lengkap + alasan penolakan + waktu karantina, supaya
-- bisa diaudit (data apa yang gagal, kenapa, seberapa sering).
--
-- Dibangun dari SUMBER RAW YANG SAMA dengan stg_trips, dan memakai
-- macro trip_rejection_flags() yang sama sehingga definisi valid
-- di kedua model dijamin identik.
--
-- Model ini sengaja bermaterialisasi VIEW (mengikuti default folder
-- staging), bukan tabel. Alasannya: `check_quarantine_surge` membutuhkan
-- rasio baris ditolak per eksekusi dbt, sehingga isinya harus selalu
-- mencerminkan data raw saat ini. Sebagai view, masa hidupnya otomatis
-- mengikuti tabel raw dan tidak perlu retensi tersendiri.
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'trips') }}
),

-- Pemetaan kanonik yang SAMA dengan stg_trips, supaya aturan DQ di kedua
-- model tidak mungkin berbeda tafsir (mis. baris yang sama dinilai valid
-- di satu model tetapi rejected di model lain).
station_map AS (
    SELECT station_id, canonical_station_id
    FROM {{ ref('stg_station_id_mapping') }}
),

cleaned AS (
    SELECT
        NULLIF(TRIM(sc.ride_id), '')                        AS ride_id,
        LOWER(NULLIF(TRIM(sc.rideable_type), ''))           AS rideable_type,
        sc.started_at,
        sc.ended_at,
        NULLIF(TRIM(sc.start_station_name), '')             AS start_station_name,
        NULLIF(TRIM(sc.end_station_name), '')               AS end_station_name,
        COALESCE(ms.canonical_station_id,
                 NULLIF(REGEXP_REPLACE(TRIM(COALESCE(sc.start_station_id, '')), r'_$', ''), ''))
                                                           AS start_station_id,
        COALESCE(me.canonical_station_id,
                 NULLIF(REGEXP_REPLACE(TRIM(COALESCE(sc.end_station_id, '')), r'_$', ''), ''))
                                                           AS end_station_id,
        sc.start_lat,
        sc.start_lng,
        sc.end_lat,
        sc.end_lng,
        LOWER(NULLIF(TRIM(sc.member_casual), ''))          AS member_casual,
        sc._source_file,
        sc._ingested_at,
        sc._ride_started_date
    FROM source AS sc
    LEFT JOIN station_map AS ms
        ON NULLIF(REGEXP_REPLACE(TRIM(COALESCE(sc.start_station_id, '')), r'_$', ''), '')
           = ms.station_id
    LEFT JOIN station_map AS me
        ON NULLIF(REGEXP_REPLACE(TRIM(COALESCE(sc.end_station_id, '')), r'_$', ''), '')
           = me.station_id
),

flagged AS (
    SELECT
        cleaned.*,
        TIMESTAMP_DIFF(ended_at, started_at, SECOND) AS duration_seconds,
        DATE(started_at)                             AS trip_date,
        {{ trip_rejection_flags() }}                 AS rejection_reason
    FROM cleaned
)

SELECT
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    duration_seconds,
    trip_date,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual,
    rejection_reason,
    _source_file,
    CURRENT_TIMESTAMP() AS quarantined_at,
    _ingested_at
FROM flagged
WHERE rejection_reason != ''
