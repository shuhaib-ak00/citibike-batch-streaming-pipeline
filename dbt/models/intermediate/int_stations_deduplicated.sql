-- ============================================================
-- int_stations_deduplicated — daftar stasiun unik
--
-- Dimensi stasiun dibangun dari titik awal DAN akhir trip, lalu di-dedup
-- menjadi satu baris per station_id.
--
-- Catatan pengembangan:
--
-- - Tidak ada penggabungan id tambahan di sini. Normalisasi suffix '_' dan
--   pemetaan ke id kanonik sudah dilakukan di stg_trips, lewat
--   stg_station_id_mapping.
--
-- - Nama stasiun bisa berubah antar waktu -> diambil nama TERBARU yang tidak
--   null (ARRAY_AGG IGNORE NULLS ORDER BY ... LIMIT 1).
--
-- - Koordinat diambil dari kemunculan terbaru yang tidak null, sehingga trip
--   dengan GPS gagal tetap punya titik di peta.
-- ============================================================

WITH stations_from_trips AS (
    SELECT
        start_station_id   AS station_id,
        start_station_name AS station_name,
        start_lat          AS lat,
        start_lng          AS lng,
        started_at         AS observed_at
    FROM {{ ref('stg_trips') }}
    WHERE start_station_id IS NOT NULL

    UNION ALL

    SELECT
        end_station_id     AS station_id,
        end_station_name   AS station_name,
        end_lat            AS lat,
        end_lng            AS lng,
        ended_at           AS observed_at
    FROM {{ ref('stg_trips') }}
    WHERE end_station_id IS NOT NULL
)

SELECT
    station_id,
    ARRAY_AGG(station_name IGNORE NULLS ORDER BY observed_at DESC LIMIT 1)[SAFE_OFFSET(0)]
        AS station_name,
    ARRAY_AGG(lat IGNORE NULLS ORDER BY observed_at DESC LIMIT 1)[SAFE_OFFSET(0)]
        AS lat,
    ARRAY_AGG(lng IGNORE NULLS ORDER BY observed_at DESC LIMIT 1)[SAFE_OFFSET(0)]
        AS lng,
    COUNT(*) AS trip_mentions,
    MIN(observed_at) AS first_seen_at,
    MAX(observed_at) AS last_seen_at
FROM stations_from_trips
GROUP BY station_id
