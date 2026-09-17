-- ============================================================
-- station_availability_realtime — mart dashboard (chart 5: peta)
--
-- Menyimpan HANYA snapshot terakhir, bukan seluruh riwayat.
--
-- Kenapa dimaterialisasi sebagai table, padahal mayoritas mart lain view:
-- chart ini di-auto-refresh tiap menit agar terlihat "hidup". Kalau berupa
-- view, setiap refresh akan memindai partisi hari ini (±240 MB) dan
-- 1.440 refresh/hari akan menghabiskan kuota query gratis 1 TiB/bulan
-- dalam ±3 hari. Sebagai table berisi ±2.500 baris (±250 KB), biaya
-- pemindaiannya turun sekitar seribu kali, sehingga auto-refresh aman
-- dinyalakan terus.
--
-- Grain: 1 baris per stasiun (kondisi terkini).
--
-- Kolom risk_* dipakai untuk mewarnai peta di dashboard, sehingga tim ops
-- bisa melihat sebaran stasiun bermasalah tanpa membuka tabel lain.
-- ============================================================

{{ config(materialized='table', tags=['marts', 'dashboard']) }}

WITH latest AS (
    -- Memakai snapshot terakhir yang LENGKAP, bukan sekadar terakhir.
    -- Penulisan consumer dapat terpotong di tengah snapshot, sehingga
    -- snapshot paling baru bisa hanya memuat sebagian kecil stasiun.
    SELECT snapshot_timestamp AS max_snapshot
    FROM {{ ref('int_latest_complete_snapshot') }}
),

current_state AS (
    SELECT f.*
    FROM {{ ref('fct_station_status') }} AS f
    INNER JOIN latest
        ON f.snapshot_timestamp = latest.max_snapshot
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
    num_ebikes_available,
    num_docks_available,
    occupancy_rate_pct,

    is_operational,
    is_stale,
    risk_level,
    risk_severity,

    -- Status ringkas untuk tooltip peta: satu kata yang langsung terbaca.
    CASE
        WHEN NOT is_operational THEN 'Tidak beroperasi'
        WHEN risk_level = 'empty'  THEN 'Kosong'
        WHEN risk_level = 'full'   THEN 'Penuh'
        WHEN risk_level = 'low'    THEN 'Sepeda sedikit'
        WHEN risk_level = 'high'   THEN 'Dock sedikit'
        WHEN risk_level = 'unknown_capacity' THEN 'Kapasitas tidak diketahui'
        ELSE 'Normal'
    END                                           AS availability_label
FROM current_state
