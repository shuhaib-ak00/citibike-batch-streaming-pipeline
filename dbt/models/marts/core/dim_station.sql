-- ============================================================
-- dim_station — dimensi stasiun
--
-- Jembatan antara dua ruang ID yang berbeda:
--
--   riwayat trip (CSV)       : id legacy -> '5343.10', 'JC116', 'HB602'
--   GBFS station_information : UUID / snowflake -> '3bfc859b-...'
--
-- Keduanya TIDAK saling mengenal: join langsung atas station_id menghasilkan
-- 0 baris cocok. Penghubungnya kolom `short_name` pada GBFS, yang justru berisi
-- id legacy tersebut (2.247 dari 2.285 id trip / 98,3% cocok).
--
-- Karena itu dimensi ini menyimpan DUA kolom id:
--   station_id       -> id legacy, dipakai fct_trips
--   gbfs_station_id  -> id GBFS, dipakai fct_station_status
--
-- 38 stasiun tidak ditemukan di GBFS (kemungkinan sudah dibongkar, atau
-- stasiun operasional seperti SYS/HB). Capacity-nya dibiarkan NULL dan
-- ditandai lewat `has_capacity_info` / `is_missing_from_gbfs`.
-- ============================================================

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
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['s.station_id']) }} AS station_key,
    s.station_id,
    i.gbfs_station_id,
    COALESCE(s.station_name, i.info_name)                    AS station_name,
    -- Koordinat dari trip dipakai lebih dulu karena berasal dari lokasi
    -- aktual pemakaian; GBFS menjadi cadangan bila trip tidak punya titik.
    COALESCE(s.lat, i.info_lat)                              AS lat,
    COALESCE(s.lng, i.info_lon)                              AS lng,
    -- Kapasitas <= 0 tidak bisa dipakai menghitung hunian (pembagian nol)
    -- dan tidak bermakna secara operasional. Diperlakukan sebagai **tidak
    -- diketahui**, bukan sebagai kapasitas nol — dua hal itu berbeda arti.
    --
    -- Data nyata: 26 stasiun dilaporkan berkapasitas 0 dan satu stasiun
    -- (E 1 St & Bowery) berkapasitas 1 padahal rutin menampung puluhan
    -- sepeda. Keduanya berasal dari feed GBFS, jadi tidak bisa diperbaiki
    -- di sisi kita — hanya bisa dihindari agar tidak merusak perhitungan.
    CASE WHEN i.capacity > 0 THEN i.capacity END             AS capacity,
    i.region_id,
    s.trip_mentions,
    s.first_seen_at,
    s.last_seen_at,
    -- Ditandai eksplisit agar capacity NULL tidak salah dibaca sebagai 0.
    (i.capacity > 0)                                         AS has_capacity_info,
    (i.short_name IS NULL)                                   AS is_missing_from_gbfs
FROM stations AS s
LEFT JOIN info_latest AS i
    ON s.station_id = i.short_name
