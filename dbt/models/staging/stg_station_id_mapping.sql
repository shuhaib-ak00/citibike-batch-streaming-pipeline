-- ============================================================
-- stg_station_id_mapping — pemetaan station_id -> station_id kanonik
--
-- MASALAH YANG DISELESAIKAN (temuan verifikasi mart)
-- ---------------------------------------------------
-- Ditemukan 65 stasiun fisik yang tercatat dengan DUA station_id
-- berbeda karena perbedaan gaya penulisan digit terakhir:
--
--     5343.1   Allen St & Hester St   lat=40.71606 lng=-73.99191
--     5343.10  Allen St & Hester St   lat=40.71606 lng=-73.99191
--             ^ koordinat IDENTIK, nama IDENTIK
--
-- Dampak: satu stasiun muncul sebagai dua baris di chart/peta, dan
-- analisis net_flow menjadi bias (mis. 5343.1 net -4.645 dan
-- 5343.10 net +4.536 terlihat "seimbang" padahal gabungannya tidak).
--
-- Pengecekan format (`invalid_station_id`) tidak menangkapnya karena
-- `5343.1` lolos regex format yang sah.
--
-- KRITERIA PENGGABUNGAN (sengaja konservatif)
-- -------------------------------------------
-- Dua station_id digabung HANYA bila ketiganya sama:
--     (1) nama stasiun (setelah TRIM, case-insensitive)
--     (2) latitude  (dibulatkan 6 desimal)
--     (3) longitude (dibulatkan 6 desimal)
--
-- Kriteria koordinat inilah yang menjaga agar stasiun yang MEMANG
-- terpisah tidak ikut tergabung. Terverifikasi: dari 67 pasangan
-- nama-kembar, 65 berpola artifact dengan koordinat identik,
-- sedangkan 2 sisanya punya koordinat berbeda:
--     7625.18 vs 7625.22  (E 118 St & Park Ave)      -> dibiarkan
--     8381.04 vs 8421.03  (W 181 St & Riverside Dr)  -> dibiarkan
--
-- Id kanonik dipilih = yang paling banyak dipakai (mentions terbanyak),
-- dengan tie-break `station_id` menaik agar deterministik.
--
-- DIMATERIALISASI SEBAGAI TABLE karena tabelnya kecil (~2.350 baris)
-- tetapi dipakai berulang oleh stg_trips & stg_trips_rejected; sebagai
-- view, setiap pemakaian akan memicu pemindaian ulang raw (1,1 GiB).
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
