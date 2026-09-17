-- ============================================================
-- fct_station_status — fact table status stasiun (periodic snapshot fact)
--
-- Grain: **1 baris per stasiun per snapshot polling**.
--
-- Ini periodic snapshot fact, bukan transaction fact seperti fct_trips.
-- Bedanya penting: setiap polling merekam kondisi SELURUH stasiun, bukan
-- hanya yang mengalami perubahan. Konsekuensinya tabel ini tumbuh cepat
-- (±2.500 baris tiap 90 detik) tetapi selalu memberi gambaran lengkap —
-- tidak perlu merekonstruksi keadaan dari selisih antar waktu.
--
-- Karena itu agregasi (mis. rata-rata hunian per jam, durasi sebuah
-- stasiun berada dalam kondisi berisiko) dilakukan dengan GROUP BY atas
-- bucket waktu, bukan dengan validity window.
--
-- Partition: harian pada snapshot_date — wajib, karena tanpa partisi
-- setiap query "kondisi terkini" akan memindai seluruh riwayat.
-- Cluster: station_key — kolom yang selalu dipakai untuk memfilter
-- stasiun tertentu.
-- ============================================================

{{ config(
    materialized='table',
    partition_by={'field': 'snapshot_date', 'data_type': 'date'},
    cluster_by=['station_key']
) }}

WITH enriched AS (
    SELECT * FROM {{ ref('int_station_risk_calculation') }}
)

SELECT
    -- Kunci surrogate: satu observasi = satu stasiun pada satu waktu.
    {{ dbt_utils.generate_surrogate_key(['gbfs_station_id', 'snapshot_timestamp']) }}
                                                        AS station_status_key,

    -- Kunci dimensi. station_key dan station_id dibiarkan NULL bila stasiun
    -- tidak dikenal, agar jelas bahwa barisnya tidak bisa dipetakan —
    -- bukan dipaksa menunjuk ke dimensi yang salah.
    station_key,
    legacy_station_id,
    gbfs_station_id,
    CAST(FORMAT_DATE('%Y%m%d', CAST(snapshot_timestamp AS DATE)) AS INT64)
                                                        AS snapshot_date_key,
    station_name,
    region_id,
    lat,
    lng,

    -- waktu
    snapshot_timestamp,
    CAST(snapshot_timestamp AS DATE)                    AS snapshot_date,
    EXTRACT(HOUR FROM snapshot_timestamp)               AS snapshot_hour,
    {{ iso_day_of_week('snapshot_timestamp') }}         AS snapshot_day_of_week,
    last_reported,
    report_age_seconds,

    -- ukuran
    num_bikes_available,
    num_ebikes_available,
    num_scooters_available,
    num_docks_available,
    num_bikes_disabled,
    num_docks_disabled,
    capacity,
    occupancy_rate_pct,

    -- status operasional
    is_installed,
    is_renting,
    is_returning,
    is_disabled,
    is_operational,
    is_unknown_station,

    -- kualitas data
    is_stale,
    -- True bila jumlah sepeda melebihi kapasitas yang dilaporkan GBFS.
    -- Baris seperti itu tetap disimpan, tetapi occupancy_rate_pct-nya
    -- dikosongkan karena rasionya tidak bisa dipercaya.
    is_capacity_inconsistent,

    -- klasifikasi
    risk_level,
    risk_severity,

    _ingested_at
FROM enriched
