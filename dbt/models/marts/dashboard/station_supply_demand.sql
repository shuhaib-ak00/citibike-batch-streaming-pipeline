-- ============================================================
-- station_supply_demand — mart dashboard (chart 7)
--
-- Chart: bar "supply vs demand per stasiun".
--
-- Ini satu-satunya mart yang menggabungkan **batch dan streaming** dalam
-- satu tampilan, dan intinya adalah mempertemukan dua hal yang tidak
-- bisa dijawab oleh salah satunya sendiri:
--
--   permintaan historis  -> "stasiun ini biasanya banyak ditinggalkan"
--   pasokan saat ini     -> "stasiun ini sekarang sepedanya menipis"
--
-- Stasiun yang memenuhi keduanya adalah prioritas tertinggi untuk
-- pengiriman sepeda. Itu jauh lebih berguna daripada melihat daftar
-- stasiun tersibuk atau daftar stasiun kosong secara terpisah.
--
-- Karena data batch berasal dari musim dingin sedangkan demo berlangsung
-- di periode lain, angka absolut antar keduanya tidak sebanding.
-- Pembacaan sebaiknya memakai `activity_rank`, `activity_share`, dan
-- `imbalance_pct` — bukan angka mentah.
--
-- Grain: 1 baris per stasiun.
-- ============================================================

{{ config(materialized='table', tags=['marts', 'dashboard']) }}

SELECT
    station_id,
    station_name,
    region_id,
    lat,
    lng,

    -- sisi permintaan (batch)
    departure_count,
    arrival_count,
    total_activity,
    net_flow,
    activity_rank,
    demand_pressure_rank,
    activity_share,

    -- sisi pasokan (streaming)
    capacity,
    num_bikes_available,
    num_docks_available,
    occupancy_rate_pct,
    risk_level,
    imbalance_pct,
    last_snapshot_at,

    -- penilaian gabungan
    is_priority_supply,
    is_priority_drain,

    -- Arah ketidakseimbangan dalam bahasa operasional.
    CASE
        WHEN net_flow > 0 AND risk_level IN ('empty', 'low')  THEN 'Kirim sepeda (permintaan tinggi, pasokan rendah)'
        WHEN net_flow < 0 AND risk_level IN ('full', 'high')  THEN 'Tarik sepeda (pasokan berlebih)'
        WHEN net_flow > 0                                     THEN 'Permintaan tinggi, pasokan cukup'
        WHEN net_flow < 0                                     THEN 'Pasokan berlebih, permintaan rendah'
        ELSE 'Seimbang'
    END                                                    AS imbalance_direction
FROM {{ ref('int_station_demand_vs_supply') }}
