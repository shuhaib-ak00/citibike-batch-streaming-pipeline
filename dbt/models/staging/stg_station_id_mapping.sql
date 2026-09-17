-- ============================================================
-- stg_station_id_mapping — station_id -> station_id kanonik
--
-- Menyatukan station_id yang sebenarnya menunjuk satu stasiun fisik, mis:
--
--     5343.1   Allen St & Hester St   lat=40.71606 lng=-73.99191
--     5343.10  Allen St & Hester St   lat=40.71606 lng=-73.99191
--
-- Dampaknya bukan sekadar kerapian: satu stasiun muncul sebagai dua baris di
-- peta, dan net_flow jadi bias (—4.645 dan +4.536 terlihat "seimbang" padahal
-- gabungannya tidak). Pengecekan format tidak menangkapnya karena `5343.1`
-- lolos regex.
--
-- Catatan pengembangan:
--
-- - Penggabungan sengaja konservatif: HANYA bila nama (TRIM, case-insensitive)
--   + latitude + longitude (dibulatkan 6 desimal) ketiganya sama. Kriteria
--   koordinat itu yang menjaga stasiun yang MEMANG terpisah tidak ikut
--   tergabung. Terbukti ada 2 pasang nama kembar berkoordinat berbeda:
--       7625.18 vs 7625.22  (E 118 St & Park Ave)     -> dibiarkan
--       8381.04 vs 8421.03  (W 181 St & Riverside Dr) -> dibiarkan
--
-- - Id kanonik = mentions terbanyak, tie-break `station_id` menaik agar
--   deterministik.
--
-- - Materialized table, bukan view: dipakai berulang oleh stg_trips dan
--   stg_trips_rejected, sedangkan sebagai view setiap pemakaian memindai ulang
--   raw (1,1 GiB).
-- ============================================================

{{ config(materialized='table', tags=['staging']) }}

WITH observations AS (
    SELECT
        start_station_id   AS station_id_raw,
        start_station_name AS station_name_raw,
        start_lat          AS lat_raw,
        start_lng          AS lng_raw
    FROM {{ source('raw', 'trips') }}

    UNION ALL

    SELECT
        end_station_id,
        end_station_name,
        end_lat,
        end_lng
    FROM {{ source('raw', 'trips') }}
),

-- Normalisasi sama persis dengan stg_trips (buang suffix '_' + TRIM)
-- agar kunci penggabungan tidak berbeda tafsir dengan layer konsumen.
normalized AS (
    SELECT
        NULLIF(REGEXP_REPLACE(TRIM(COALESCE(station_id_raw, '')), r'_$', ''), '')
            AS station_id,
        NULLIF(TRIM(station_name_raw), '')   AS station_name,
        ROUND(lat_raw, 6)                    AS lat,
        ROUND(lng_raw, 6)                    AS lng
    FROM observations
),

per_id_combo AS (
    SELECT
        station_id,
        station_name,
        lat,
        lng,
        COUNT(*) AS mentions
    FROM normalized
    WHERE station_id IS NOT NULL
    GROUP BY station_id, station_name, lat, lng
),

-- Profil tiap station_id: pakai nama & koordinat yang paling sering muncul
station_profile AS (
    SELECT
        station_id,
        ARRAY_AGG(station_name IGNORE NULLS ORDER BY mentions DESC, station_name LIMIT 1)[SAFE_OFFSET(0)]
            AS station_name,
        ARRAY_AGG(lat IGNORE NULLS ORDER BY mentions DESC LIMIT 1)[SAFE_OFFSET(0)]
            AS lat,
        ARRAY_AGG(lng IGNORE NULLS ORDER BY mentions DESC LIMIT 1)[SAFE_OFFSET(0)]
            AS lng,
        SUM(mentions) AS total_mentions
    FROM per_id_combo
    GROUP BY station_id
),

-- Id kanonik per kelompok (nama + koordinat).
-- Hanya kelompok dengan koordinat lengkap yang boleh digabung.
canonical AS (
    SELECT
        station_name,
        lat,
        lng,
        ARRAY_AGG(station_id ORDER BY total_mentions DESC, station_id LIMIT 1)[SAFE_OFFSET(0)]
            AS canonical_station_id,
        COUNT(*) AS n_ids_in_group
    FROM station_profile
    WHERE station_name IS NOT NULL
      AND lat IS NOT NULL
      AND lng IS NOT NULL
    GROUP BY station_name, lat, lng
)

SELECT
    p.station_id,
    COALESCE(c.canonical_station_id, p.station_id) AS canonical_station_id,
    p.station_name,
    p.lat,
    p.lng,
    p.total_mentions,
    -- True bila id ini merupakan duplikat yang digabung ke id lain.
    -- Dipakai untuk metrik/audit: berapa banyak id duplikat yang dirapikan.
    COALESCE(c.canonical_station_id, p.station_id) != p.station_id
        AS is_merged_duplicate,
    COALESCE(c.n_ids_in_group, 1) AS ids_in_group
FROM station_profile AS p
LEFT JOIN canonical AS c
    ON p.station_name = c.station_name
   AND p.lat = c.lat
   AND p.lng = c.lng
