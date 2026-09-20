-- ============================================================
-- int_station_risk_calculation — occupancy & klasifikasi risiko
--
-- Menghubungkan status stasiun dengan pemetaan stasiun untuk memperoleh
-- `capacity`, lalu menghitung occupancy dan mengklasifikasikan stasiun.
--
-- Catatan pengembangan:
--
-- - Membaca int_station_id_bridge, BUKAN dim_station. Tujuannya agar layer
--   intermediate tidak bergantung pada marts/core; pemetaan id dan capacity
--   berasal dari satu tabel yang juga dipakai dim_station. JANGAN salin
--   pemetaan itu ke sini — akan menjadi dua sumber kebenaran yang bisa
--   menyimpang.
--
-- - `is_operational` wajib ada. Terbukti dari data: 57 stasiun rutin
--   melaporkan bikes=0 & docks=0 dengan is_renting=false. Tanpa penanda ini
--   mereka muncul sebagai "empty paling parah" dan mendominasi daftar
--   rebalancing — sinyal palsu.
--
-- - `is_stale` menandai `last_reported` yang lebih tua dari ambang.
--
-- - `is_unknown_station` menandai stasiun yang ada di feed tetapi tidak ada di
--   dimensi. Tetap disimpan agar bisa diaudit.
--
-- Ambang klasifikasi dari var (lihat dbt_project.yml), dikalibrasi dari
-- distribusi nyata: p25=21%, p50=48%, p75=74%.
-- ============================================================

WITH status AS (
    SELECT * FROM {{ ref('stg_station_status') }}
),

stations AS (
    SELECT
        gbfs_station_id,
        station_key,
        station_id      AS legacy_station_id,
        station_name,
        lat,
        lng,
        capacity,
        region_id
    FROM {{ ref('int_station_id_bridge') }}
    WHERE gbfs_station_id IS NOT NULL
),

joined AS (
    SELECT
        s.station_id                                AS gbfs_station_id,
        s.num_bikes_available,
        s.num_ebikes_available,
        s.num_scooters_available,
        s.num_docks_available,
        s.num_bikes_disabled,
        s.num_docks_disabled,
        s.is_installed,
        s.is_renting,
        s.is_returning,
        s.is_disabled,
        s.last_reported,
        s.snapshot_timestamp,
        s.report_age_seconds,
        s._ingested_at,
        s._snapshot_date,

        d.station_key,
        d.legacy_station_id,
        d.station_name,
        d.lat,
        d.lng,
        d.capacity,
        d.region_id,
        (d.gbfs_station_id IS NULL)                 AS is_unknown_station
    FROM status AS s
    LEFT JOIN stations AS d
        ON s.station_id = d.gbfs_station_id
),

flagged AS (
    SELECT
        *,
        -- Stasiun dianggap beroperasi hanya bila semua flag mengizinkan.
        -- is_disabled TRUE berarti stasiun sengaja dinonaktifkan.
        COALESCE(is_installed, FALSE)
            AND COALESCE(is_renting, FALSE)
            AND COALESCE(is_returning, FALSE)
            AND NOT COALESCE(is_disabled, FALSE)     AS is_operational,

        report_age_seconds > {{ var('station_freshness_threshold_minutes') }} * 60
                                                     AS is_stale,

        -- Kapasitas dianggap tidak konsisten bila jumlah sepeda melebihi
        -- kapasitas yang dilaporkan. Ini terjadi pada data nyata: satu
        -- stasiun dilaporkan berkapasitas 1 padahal rutin menampung
        -- puluhan sepeda. Rasio hunian dari kapasitas seperti itu tidak
        -- bisa dipercaya, sehingga harus dikosongkan — bukan dipotong
        -- menjadi 100%, karena itu akan menyembunyikan masalahnya.
        (capacity IS NOT NULL AND num_bikes_available > capacity)
                                                     AS is_capacity_inconsistent
    FROM joined
),

with_occupancy AS (
    SELECT
        *,
        -- Hunian = proporsi sepeda terhadap kapasitas. NULL bila kapasitas
        -- tidak diketahui ATAU kapasitasnya tidak konsisten; sengaja tidak
        -- diisi 0, karena 0 berarti "stasiun kosong" — arti operasionalnya
        -- sangat berbeda.
        CASE
            WHEN capacity IS NOT NULL AND capacity > 0
             AND NOT is_capacity_inconsistent
            THEN SAFE_DIVIDE(num_bikes_available, capacity) * 100
        END                                          AS occupancy_rate_pct
    FROM flagged
)

SELECT
    *,
    CASE
        WHEN NOT is_operational              THEN 'not_operational'
        WHEN occupancy_rate_pct IS NULL      THEN 'unknown_capacity'
        -- empty/full didahulukan karena berdampak langsung ke pengguna:
        -- tidak bisa mengambil sepeda / tidak bisa mengembalikan sepeda.
        WHEN num_bikes_available = 0         THEN 'empty'
        WHEN num_docks_available = 0         THEN 'full'
        WHEN occupancy_rate_pct < {{ var('station_low_occupancy_pct') }}  THEN 'low'
        WHEN occupancy_rate_pct > {{ var('station_high_occupancy_pct') }} THEN 'high'
        ELSE 'balanced'
    END                                              AS risk_level,

    -- Tingkat keparahan untuk pengurutan di dashboard.
    -- Stasiun yang tidak beroperasi diberi 0 agar tidak pernah naik ke atas.
    CASE
        WHEN NOT is_operational              THEN 0
        WHEN occupancy_rate_pct IS NULL      THEN 1
        WHEN num_bikes_available = 0         THEN 5
        WHEN num_docks_available = 0         THEN 5
        WHEN occupancy_rate_pct < {{ var('station_low_occupancy_pct') }}  THEN 3
        WHEN occupancy_rate_pct > {{ var('station_high_occupancy_pct') }} THEN 3
        ELSE 1
    END                                              AS risk_severity
FROM with_occupancy
