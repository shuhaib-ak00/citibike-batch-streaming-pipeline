-- ============================================================
-- stg_station_status_rejected — tabel karantina station_status
--
-- Menyimpan baris status stasiun yang gagal validasi, lengkap dengan alasan
-- penolakan dan waktu karantina, supaya bisa diaudit.
--
-- Dibangun dari SUMBER RAW YANG SAMA dengan stg_station_status dan memakai
-- macro station_status_rejection_flags() yang sama, sehingga definisi valid
-- di kedua model dijamin identik.
--
-- Catatan kasus nyata: feed GBFS mengirim `last_reported` bernilai epoch
-- 86400 (1970-01-02) untuk stasiun yang tidak melaporkan status. Nilai itu
-- bukan waktu nyata, sehingga harus dikarantina agar tidak merusak
-- perhitungan umur data dan analisis risiko.
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'station_status') }}
),

cleaned AS (
    SELECT
        NULLIF(TRIM(station_id), '')                      AS station_id,
        num_bikes_available,
        num_ebikes_available,
        num_scooters_available,
        num_docks_available,
        num_bikes_disabled,
        num_docks_disabled,
        is_installed,
        is_renting,
        is_returning,
        is_disabled,
        last_reported,
        snapshot_timestamp,
        _ingested_at,
        _snapshot_date
    FROM source
),

flagged AS (
    SELECT
        cleaned.*,
        {{ station_status_rejection_flags() }} AS rejection_reason
    FROM cleaned
)

SELECT
    station_id,
    num_bikes_available,
    num_docks_available,
    is_installed,
    is_renting,
    is_returning,
    is_disabled,
    last_reported,
    snapshot_timestamp,
    rejection_reason,
    CURRENT_TIMESTAMP() AS quarantined_at,
    _ingested_at
FROM flagged
WHERE rejection_reason != ''
