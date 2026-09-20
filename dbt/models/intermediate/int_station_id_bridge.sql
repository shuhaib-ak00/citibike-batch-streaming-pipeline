-- ============================================================
-- int_station_id_bridge — pemetaan id legacy <-> id GBFS + atribut stasiun
--
-- Titik pertemuan dua ruang ID. Dipisah dari dim_station supaya layer
-- intermediate tidak perlu membaca marts/core. Dua konsumennya:
--
--   dim_station                  -> dimensi presentasi (baca 1:1 dari sini)
--   int_station_risk_calculation -> butuh pemetaan id + capacity
--
-- Catatan pengembangan:
--
-- - WAJIB table, bukan view. int_station_risk_calculation berjalan tiap jam
--   dan membacanya; bila view, pemindaian riwayat trip di bawahnya
--   (int_stations_deduplicated -> stg_trips -> raw.trips ~1,1 GiB) terulang
--   tiap jam.
--
-- - Penghubungnya kolom `short_name` pada station_information, yang justru
--   berisi id legacy itu sendiri. Tanpa kolom itu, join langsung antara
--   riwayat trip dan feed GBFS menghasilkan 0 baris cocok.
--
-- - `capacity <= 0` diperlakukan sebagai NULL (tidak diketahui), bukan 0.
--   Alasannya ada di CASE kapasitas di bawah.
--
-- - `has_capacity_info` DITURUNKAN dari kolom `capacity`, bukan dihitung ulang
--   dari `i.capacity`. Dua ekspresi terpisah yang seharusnya selalu sepakat
--   adalah sumber bug: pernah ditulis `(i.capacity > 0)`, dan karena
--   `NULL > 0` bernilai NULL (bukan FALSE) flag itu jadi NULL untuk 38
--   stasiun yang tidak ada di feed GBFS -- sehingga `WHERE NOT has_capacity_info`
--   diam-diam kehilangan 38 baris. Sebagai turunan, divergensi jadi mustahil.
--
-- - 38 stasiun dipakai di riwayat trip tetapi tidak ada di feed GBFS:
--   gbfs_station_id NULL, ditandai lewat is_missing_from_gbfs.
--
-- Grain: 1 baris per station_id (id legacy).
-- ============================================================

{{ config(materialized='table') }}

WITH stations AS (
    SELECT * FROM {{ ref('int_stations_deduplicated') }}
),

info AS (
    SELECT
        station_id      AS gbfs_station_id,
        short_name,
        name            AS info_name,
        capacity,
        region_id,
        lat             AS info_lat,
        lon             AS info_lon
    FROM {{ source('raw', 'station_information') }}
    WHERE short_name IS NOT NULL
),

-- Safety net: bila kelak tabel referensi memuat lebih dari satu snapshot,
-- ambil satu baris per short_name.
info_latest AS (
    SELECT * FROM info
    QUALIFY ROW_NUMBER() OVER (PARTITION BY short_name ORDER BY gbfs_station_id) = 1
),

gabungan AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['s.station_id']) }} AS station_key,
        s.station_id,
        i.gbfs_station_id,
        COALESCE(s.station_name, i.info_name)  AS station_name,
        -- Koordinat dari trip dipakai lebih dulu karena berasal dari lokasi
        -- aktual pemakaian; GBFS menjadi cadangan bila trip tidak punya titik.
        COALESCE(s.lat, i.info_lat)            AS lat,
        COALESCE(s.lng, i.info_lon)            AS lng,
        -- Kapasitas <= 0 tidak bisa dipakai menghitung hunian (pembagian nol)
        -- dan tidak bermakna secara operasional. Diperlakukan sebagai **tidak
        -- diketahui**, bukan sebagai kapasitas nol — dua hal itu berbeda arti.
        --
        -- Data nyata: 24 stasiun dilaporkan berkapasitas 0 dan satu stasiun
        -- (E 1 St & Bowery) berkapasitas 1 padahal rutin menampung puluhan
        -- sepeda. Keduanya berasal dari feed GBFS, jadi tidak bisa diperbaiki
        -- di sisi kita — hanya bisa dihindari agar tidak merusak perhitungan.
        CASE WHEN i.capacity > 0 THEN i.capacity END AS capacity,
        i.region_id,
        s.trip_mentions,
        s.first_seen_at,
        s.last_seen_at,
        (i.short_name IS NULL)                 AS is_missing_from_gbfs
    FROM stations AS s
    LEFT JOIN info_latest AS i
        ON s.station_id = i.short_name
)

SELECT
    *,
    -- Ditandai eksplisit agar `capacity` NULL tidak salah dibaca sebagai 0.
    --
    -- Diturunkan dari kolom `capacity` di atas, BUKAN dihitung ulang dari
    -- `i.capacity`. `IS NOT NULL` selalu mengembalikan TRUE atau FALSE,
    -- sehingga flag ini tidak mungkin NULL.
    (capacity IS NOT NULL) AS has_capacity_info
FROM gabungan
