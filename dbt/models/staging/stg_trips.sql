-- ============================================================
-- stg_trips — staging trip history (batch)
--
-- Cleaning + penyelarasan tipe. Grain tetap 1:1 dengan raw, tanpa agregasi.
--
--   1. Trim string, normalisasi tipe & kapitalisasi.
--   2. Normalisasi station_id: buang suffix '_' ('5303.06_' = '5303.06').
--   3. Petakan station_id ke bentuk kanonik lewat stg_station_id_mapping,
--      sehingga 65 stasiun ber-id ganda (mis. 5343.1 dan 5343.10 dengan nama &
--      koordinat identik) menjadi satu id saja.
--   4. Hitung duration_seconds & trip_date.
--   5. Saring baris VALID saja (macro trip_rejection_flags).
--   6. Dedup ride_id — tidak dikarantina, jumlahnya dicatat di
--      int_trips_dq_summary.
--
-- Baris gagal DQ TIDAK dibuang — lihat stg_trips_rejected.
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'trips') }}
),

-- Pemetaan id -> id kanonik, dibangun dari sumber raw yang sama
-- (bukan dari stg_trips) supaya tidak ada dependensi sirkular.
station_map AS (
    SELECT station_id, canonical_station_id
    FROM {{ ref('stg_station_id_mapping') }}
),

cleaned AS (
    SELECT
        NULLIF(TRIM(ride_id), '')                          AS ride_id,
        LOWER(NULLIF(TRIM(rideable_type), ''))             AS rideable_type,
        started_at,
        ended_at,

        -- Nama stasiun: trim spasi di ujung
        NULLIF(TRIM(start_station_name), '')               AS start_station_name,
        NULLIF(TRIM(end_station_name), '')                 AS end_station_name,

        -- ID stasiun: trim -> buang suffix '_' -> petakan ke id kanonik.
        -- COALESCE menjaga baris tetap aman bila id tidak ada di mapping.
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
),

valid AS (
    SELECT * FROM flagged
    WHERE rejection_reason = ''
),

deduplicated AS (
    SELECT
        * EXCEPT (rejection_reason)
    FROM valid
    -- Duplikasi di-dedup langsung, bukan dikarantina.
    -- Ambil baris dengan _ingested_at terbaru agar re-ingest menang.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ride_id
        ORDER BY _ingested_at DESC, ended_at DESC
    ) = 1
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
    _source_file,
    _ingested_at
FROM deduplicated
