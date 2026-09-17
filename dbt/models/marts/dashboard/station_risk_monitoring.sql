-- ============================================================
-- station_risk_monitoring — mart dashboard (chart 6: tabel risiko)
--
-- Daftar stasiun yang perlu perhatian tim ops SAAT INI, diurutkan dari yang
-- paling mendesak — kandidat rebalancing.
--
-- Sumbernya snapshot terakhir. Bila memakai riwayat, tabel ini akan memuat
-- ribuan baris per stasiun dan tidak lagi menjawab "stasiun mana yang perlu
-- ditangani sekarang?".
--
-- Hanya stasiun yang benar-benar beroperasi yang disertakan:
--
--   is_operational = TRUE -> stasiun non-operasional bukan masalah rebalancing
--     melainkan masalah inventaris; menyertakannya memenuhi tabel dengan baris
--     yang tidak bisa ditindaklanjuti.
--
--   is_stale = FALSE -> angka lama tidak bisa dipercaya sebagai kondisi
--     terkini, jadi tidak boleh jadi dasar keputusan.
--
-- Grain: 1 baris per stasiun berisiko.
-- ============================================================

{{ config(materialized='table', tags=['marts', 'dashboard']) }}

WITH latest AS (
    -- Snapshot terakhir yang LENGKAP — lihat int_latest_complete_snapshot.
    -- Memakai snapshot terakhir secara buta bisa menghasilkan tabel kosong
    -- bila penulisan consumer terpotong di tengah snapshot.
    SELECT snapshot_timestamp AS max_snapshot
    FROM {{ ref('int_latest_complete_snapshot') }}
),

current_state AS (
    SELECT f.*
    FROM {{ ref('fct_station_status') }} AS f
    INNER JOIN latest
        ON f.snapshot_timestamp = latest.max_snapshot
),

operational AS (
    SELECT *
    FROM current_state
    WHERE is_operational
      AND NOT is_stale
      AND capacity IS NOT NULL
      AND capacity > 0
)

SELECT
    gbfs_station_id,
    legacy_station_id,
    station_key,
    station_name,
    region_id,
    lat,
    lng,
    capacity,

    snapshot_timestamp                            AS last_snapshot_at,
    last_reported,
    report_age_seconds,

    num_bikes_available,
    num_docks_available,
    occupancy_rate_pct,
    risk_level,
    risk_severity,

    -- Arah tindakan yang perlu diambil. Ini yang membuat tabel bisa
    -- langsung dipakai, bukan sekadar menampilkan angka.
    CASE
        WHEN risk_level IN ('empty', 'low')  THEN 'Kirim sepeda ke stasiun ini'
        WHEN risk_level IN ('full', 'high')  THEN 'Tarik sepeda dari stasiun ini'
        ELSE 'Pantau'
    END                                           AS recommended_action,

    -- Berapa sepeda yang perlu dipindahkan agar hunian kembali ke 50%.
    -- Dibulatkan ke atas dan minimal 1, karena pemindahan 0 tidak berarti.
    CASE
        WHEN risk_level IN ('empty', 'low')
            THEN GREATEST(1, CAST(ROUND(capacity * 0.5) AS INT64) - num_bikes_available)
        WHEN risk_level IN ('full', 'high')
            THEN GREATEST(1, num_bikes_available - CAST(ROUND(capacity * 0.5) AS INT64))
    END                                           AS bikes_to_move,

    -- Peringkat untuk urutan tampil di dashboard. Memakai ROW_NUMBER, bukan
    -- RANK, supaya setiap baris punya nomor unik — kalau memakai RANK,
    -- puluhan stasiun yang sama-sama kosong akan berbagi peringkat #1 dan
    -- tabelnya jadi sulit dibaca.
    ROW_NUMBER() OVER (
        ORDER BY risk_severity DESC,
                 occupancy_rate_pct ASC,
                 capacity DESC,
                 gbfs_station_id
    )                                                 AS risk_rank
FROM operational
WHERE risk_level IN ('empty', 'low', 'high', 'full')
