-- ============================================================
-- int_station_demand_vs_supply — pertemuan batch & streaming
--
-- Mempertemukan permintaan historis (riwayat trip) dengan pasokan terkini
-- (snapshot streaming):
--
--     "Stasiun mana yang secara historis banyak ditinggalkan, tetapi
--      saat ini justru kekurangan sepeda?"
--
-- Catatan pengembangan:
--
-- - Model ini adalah SATU-SATUNYA sumber definisi net_flow per stasiun. Mart
--   lain yang membutuhkannya membaca dari sini — jangan duplikasi rumusnya.
--
-- - Normalisasi musiman: data batch dari Jan-Mar (musim dingin), sedangkan
--   dashboard mungkin menampilkan periode berbeda. Karena itu angka absolut
--   tidak sebanding antar musim; pakai kolom *_rank dan *_ratio.
--
-- - LEFT JOIN dari sisi permintaan supaya mart batch tetap utuh walau
--   streaming belum berjalan.
--
-- Grain: 1 baris per stasiun.
-- ============================================================

WITH trips AS (
    SELECT * FROM {{ ref('fct_trips') }}
),

demand AS (
    SELECT
        start_station_id AS station_id,
        COUNT(*)         AS departure_count
    FROM trips
    WHERE start_station_id IS NOT NULL
    GROUP BY start_station_id
),

arrivals AS (
    SELECT
        end_station_id   AS station_id,
        COUNT(*)         AS arrival_count
    FROM trips
    WHERE end_station_id IS NOT NULL
    GROUP BY end_station_id
),

demand_combined AS (
    SELECT
        COALESCE(d.station_id, a.station_id)      AS station_id,
        COALESCE(d.departure_count, 0)            AS departure_count,
        COALESCE(a.arrival_count, 0)              AS arrival_count,
        COALESCE(d.departure_count, 0)
            + COALESCE(a.arrival_count, 0)        AS total_activity,
        -- Positif berarti stasiun cenderung mengosongkan sepeda
        -- (lebih banyak berangkat daripada datang).
        COALESCE(d.departure_count, 0)
            - COALESCE(a.arrival_count, 0)        AS net_flow
    FROM demand AS d
    FULL OUTER JOIN arrivals AS a
        ON d.station_id = a.station_id
),

-- Pasokan terkini: snapshot terakhir yang LENGKAP, karena pertanyaannya
-- adalah "bagaimana kondisinya SEKARANG". Memakai snapshot terakhir secara
-- buta berisiko mengambil snapshot parsial (lihat int_latest_complete_snapshot).
latest_snapshot AS (
    SELECT snapshot_timestamp AS max_snapshot
    FROM {{ ref('int_latest_complete_snapshot') }}
),

supply AS (
    SELECT
        legacy_station_id,
        station_name,
        region_id,
        lat,
        lng,
        capacity,
        num_bikes_available,
        num_docks_available,
        occupancy_rate_pct,
        risk_level,
        risk_severity,
        is_operational,
        is_stale,
        snapshot_timestamp AS last_snapshot_at
    FROM {{ ref('int_station_risk_calculation') }}
    WHERE snapshot_timestamp = (SELECT max_snapshot FROM latest_snapshot)
),

combined AS (
    SELECT
        d.station_id,
        d.departure_count,
        d.arrival_count,
        d.total_activity,
        d.net_flow,
        s.station_name,
        s.region_id,
        s.lat,
        s.lng,
        s.capacity,
        s.num_bikes_available,
        s.num_docks_available,
        s.occupancy_rate_pct,
        s.risk_level,
        s.risk_severity,
        s.is_operational,
        s.is_stale,
        s.last_snapshot_at
    FROM demand_combined AS d
    LEFT JOIN supply AS s
        ON d.station_id = s.legacy_station_id
)

SELECT
    *,
    -- Peringkat relatif: dipakai sebagai pembanding antar musim.
    RANK() OVER (ORDER BY total_activity DESC)     AS activity_rank,
    RANK() OVER (ORDER BY net_flow DESC)           AS demand_pressure_rank,

    -- Pangsa aktivitas stasiun terhadap total: 0..1
    ROUND(SAFE_DIVIDE(total_activity, SUM(total_activity) OVER ()), 6)
                                                   AS activity_share,

    -- Ketidakselarasan: seberapa jauh hunian menyimpang dari titik
    -- seimbang (50%). Dipakai untuk menandai stasiun yang paling
    -- membutuhkan tindakan, terlepas dari arah penyimpangannya.
    CASE WHEN occupancy_rate_pct IS NOT NULL
         THEN ROUND(ABS(occupancy_rate_pct - 50), 2)
    END                                            AS imbalance_pct,

    -- Stasiun yang historis banyak ditinggalkan DAN saat ini menipis:
    -- inilah prioritas tertinggi untuk pengiriman sepeda.
    (net_flow > 0 AND risk_level IN ('empty', 'low'))  AS is_priority_supply,
    -- Kebalikannya: historis banyak didatangi DAN saat ini penuh.
    (net_flow < 0 AND risk_level IN ('full', 'high'))  AS is_priority_drain
FROM combined
