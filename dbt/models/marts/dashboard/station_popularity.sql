-- ============================================================
-- station_popularity — mart dashboard #2
-- Chart: bar "top 10 stasiun tersibuk".
--
-- Aktivitas stasiun = keberangkatan + kedatangan, karena satu perjalanan
-- menyentuh dua stasiun. Memakai hanya keberangkatan akan melewatkan
-- stasiun yang berperan sebagai tujuan utama.
--
-- Angka permintaan (departure/arrival/net_flow) diambil dari
-- int_station_demand_vs_supply, bukan dihitung ulang di sini, supaya
-- hanya ada SATU definisi net_flow di seluruh project. Mart ini tidak
-- menampilkan kolom pasokan karena fokusnya riwayat batch.
--
-- Grain: 1 baris per stasiun.
-- ============================================================

SELECT
    station_id,
    station_name,
    lat,
    lng,
    departure_count,
    arrival_count,
    total_activity,
    net_flow,
    activity_rank
FROM {{ ref('int_station_demand_vs_supply') }}
