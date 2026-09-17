-- ============================================================
-- stg_station_status — staging status stasiun (streaming)
--
-- Cleaning + penyelarasan tipe. Grain tetap 1:1 dengan raw: 1 baris per
-- stasiun per snapshot polling.
--
--   1. Normalisasi tipe: flag 0/1 GBFS -> BOOLEAN, string dipangkas.
--   2. Saring baris VALID saja (macro station_status_rejection_flags).
--   3. Dedup berdasarkan (station_id, last_reported).
--
-- Catatan pengembangan:
--
-- - Dedup memakai (station_id, last_reported), bukan snapshot saja. Kafka
--   bersifat at-least-once sehingga pesan bisa terkirim ulang; `last_reported`
--   hanya berubah saat operator memperbarui status, jadi kombinasi itu
--   menandai satu observasi unik.
--
-- - Tabel raw bersifat APPEND-ONLY (consumer menulis terus-menerus, tidak
--   pernah dihapus), sehingga dedup di sini yang menjaga grain. Berbeda dari
--   tabel trips yang dimuat ulang per partisi.
--
-- Baris gagal DQ tidak dibuang — lihat stg_station_status_rejected.
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

        -- GBFS mengirim 0/1, sebagian operator mengirim true/false.
        -- Dibiarkan NULL bila field tidak ada, agar bisa dibedakan dari false.
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
),

valid AS (
    SELECT * FROM flagged
    WHERE rejection_reason = ''
),

deduplicated AS (
    SELECT
        * EXCEPT (rejection_reason)
    FROM valid
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY station_id, last_reported
        ORDER BY _ingested_at DESC, snapshot_timestamp DESC
    ) = 1
)

SELECT
    station_id,
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
    -- Selisih waktu antara saat data dilaporkan operator dan saat kita
    -- mem-poll-nya. Dipakai sebagai dasar freshness check.
    TIMESTAMP_DIFF(snapshot_timestamp, last_reported, SECOND) AS report_age_seconds,
    _ingested_at,
    _snapshot_date
FROM deduplicated
