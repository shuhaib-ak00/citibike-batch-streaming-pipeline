-- ============================================================
-- stg_station_status — staging status stasiun (streaming)
--
-- Peran: cleaning + penyelarasan tipe. Grain tetap 1:1 dengan raw, yaitu
-- 1 baris per stasiun per snapshot polling.
--
-- Yang dilakukan:
--   1. Normalisasi tipe: flag 0/1 GBFS menjadi BOOLEAN, string dipangkas.
--   2. Saring baris VALID saja (aturan DQ dari macro
--      station_status_rejection_flags).
--   3. Dedup berdasarkan (station_id, last_reported).
--
-- Kenapa dedup memakai kunci itu, bukan ride/snapshot saja:
-- Kafka bersifat at-least-once, sehingga pesan yang sama bisa terkirim
-- lebih dari sekali. Nilai `last_reported` hanya berubah ketika operator
-- memperbarui status stasiun, jadi kombinasi (station_id, last_reported)
-- menandai satu **observasi** yang unik. Bila polling berikutnya masih
-- membawa last_reported yang sama, itu memang observasi yang sama —
-- bukan data baru yang boleh dihitung dua kali.
--
-- Baris yang gagal DQ tidak dibuang: lihat stg_station_status_rejected.
--
-- Catatan: tabel raw bersifat APPEND-ONLY (consumer menulis terus-menerus),
-- sehingga dedup di sini berperan penting untuk menjaga grain. Berbeda dari
-- tabel trips yang dimuat ulang per partisi, tabel ini tidak pernah dihapus.
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
