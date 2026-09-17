-- ============================================================
-- int_station_status_changes — arsip PERUBAHAN per stasiun (snapshot-diff CDC)
--
-- Hanya baris yang benar-benar berubah yang disimpan, berbeda dari
-- fct_station_status yang menyimpan seluruh snapshot.
--
-- Catatan pengembangan:
--
-- - Kunci diff WAJIB `gbfs_station_id`, BUKAN `station_key`. Di
--   fct_station_status, `station_key` NULL untuk ~10% baris (stasiun tanpa
--   riwayat trip), dan `PARTITION BY` kolom ber-NULL menaruh semua NULL dalam
--   SATU partisi -- LAG() lalu membandingkan stasiun yang berbeda.
--
-- - Wajib ada BENIH dari snapshot di batas pemrosesan terakhir. `LAG()` hanya
--   melihat baris di dataset input, jadi tanpa benih baris pertama tiap batch
--   dianggap "stasiun baru" (~2.500 event op='c' palsu per rotasi). Benih
--   diambil per-snapshot, bukan per-stasiun, agar stasiun yang belum pernah
--   berubah tetap tercakup.
--
-- - `insert_overwrite` + `copy_partitions=True`. Adapter BigQuery hanya
--   menerima 'merge' dan 'insert_overwrite'; `merge` memindai ~130 MiB per run
--   (~94 GB/bulan) hanya untuk menuliskan 0 baris. Tanpa `copy_partitions`,
--   insert_overwrite MENGGANTI seluruh partisi yang tersentuh.
--
-- - Idempotensi bergantung pada filter `snapshot_timestamp > batas`, bukan
--   kunci unik. Duplikat ditangkap test unique_combination_of_columns.
--
-- - Test `assert_station_status_changes_single_create` menangkap benih kosong,
--   mis. bila snapshot batas sudah terhapus retensi.
--
-- Grain: 1 baris per PERUBAHAN per stasiun.
-- ============================================================

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    copy_partitions=True,
    partition_by={'field': 'snapshot_date', 'data_type': 'date'},
    cluster_by=['gbfs_station_id']
) }}

-- ------------------------------------------------------------------
-- Benih: keadaan seluruh stasiun pada snapshot batas pemrosesan.
-- Hanya diperlukan saat run incremental; pada run pertama semua baris
-- memang baru, sehingga `bikes_sebelumnya` boleh NULL.
-- ------------------------------------------------------------------
WITH arsip AS (
    {% if is_incremental() %}
    SELECT
        gbfs_station_id,
        snapshot_timestamp,
        num_bikes_available,
        num_docks_available,
        num_ebikes_available,
        num_scooters_available,
        is_installed,
        is_renting,
        is_returning,
        is_disabled,
        -- Kolom metadata dibiarkan NULL pada benih: ia hanya dipakai agar
        -- LAG punya pendahulu, dan barisnya tidak ikut tersimpan.
        CAST(NULL AS STRING)    AS station_key,
        CAST(NULL AS STRING)    AS legacy_station_id,
        CAST(NULL AS STRING)    AS station_name,
        CAST(NULL AS STRING)    AS risk_level,
        CAST(NULL AS BOOL)      AS is_operational,
        CAST(NULL AS BOOL)      AS is_unknown_station,
        CAST(NULL AS TIMESTAMP) AS last_reported,
        CAST(NULL AS INT64)     AS report_age_seconds,
        FALSE                   AS _baru
    FROM {{ ref('fct_station_status') }}
    WHERE snapshot_timestamp = (SELECT MAX(valid_from) FROM {{ this }})
    {% else %}
    SELECT
        CAST(NULL AS STRING)    AS gbfs_station_id,
        CAST(NULL AS TIMESTAMP) AS snapshot_timestamp,
        CAST(NULL AS INT64)     AS num_bikes_available,
        CAST(NULL AS INT64)     AS num_docks_available,
        CAST(NULL AS INT64)     AS num_ebikes_available,
        CAST(NULL AS INT64)     AS num_scooters_available,
        CAST(NULL AS BOOL)      AS is_installed,
        CAST(NULL AS BOOL)      AS is_renting,
        CAST(NULL AS BOOL)      AS is_returning,
        CAST(NULL AS BOOL)      AS is_disabled,
        CAST(NULL AS STRING)    AS station_key,
        CAST(NULL AS STRING)    AS legacy_station_id,
        CAST(NULL AS STRING)    AS station_name,
        CAST(NULL AS STRING)    AS risk_level,
        CAST(NULL AS BOOL)      AS is_operational,
        CAST(NULL AS BOOL)      AS is_unknown_station,
        CAST(NULL AS TIMESTAMP) AS last_reported,
        CAST(NULL AS INT64)     AS report_age_seconds,
        FALSE                   AS _baru
    -- `FROM (SELECT 1)` diperlukan agar klausa WHERE sah: BigQuery menolak
    -- "Query without FROM clause cannot have a WHERE clause". Baris palsu
    -- ini tidak pernah ikut, karena WHERE FALSE.
    FROM (SELECT 1)
    WHERE FALSE
    {% endif %}
),

-- ------------------------------------------------------------------
-- Snapshot baru yang belum pernah diproses.
--
-- Kolom metadata dibawa sejak di sini, alih-alih di-join di akhir.
-- Alasannya biaya: join di akhir memaksa `fct_station_status` dipindai
-- DUA kali dalam satu query. Terverifikasi di log — tanpa perbaikan ini
-- query memproses 130,2 MiB untuk menghasilkan 0 baris baru.
-- ------------------------------------------------------------------
snapshot_baru AS (
    SELECT
        gbfs_station_id,
        snapshot_timestamp,
        num_bikes_available,
        num_docks_available,
        num_ebikes_available,
        num_scooters_available,
        is_installed,
        is_renting,
        is_returning,
        is_disabled,
        station_key,
        legacy_station_id,
        station_name,
        risk_level,
        is_operational,
        is_unknown_station,
        last_reported,
        report_age_seconds,
        TRUE AS _baru
    FROM {{ ref('fct_station_status') }}
    {% if is_incremental() %}
    WHERE snapshot_timestamp > (SELECT MAX(valid_from) FROM {{ this }})
    {% endif %}
),

gabungan AS (
    SELECT * FROM arsip
    UNION ALL
    SELECT * FROM snapshot_baru
),

-- ------------------------------------------------------------------
-- Bandingkan tiap baris dengan keadaan sebelumnya untuk stasiun yang sama.
-- ------------------------------------------------------------------
berurutan AS (
    SELECT
        *,
        LAG(num_bikes_available)  OVER w AS bikes_sebelumnya,
        LAG(num_docks_available)  OVER w AS docks_sebelumnya,
        LAG(num_ebikes_available) OVER w AS ebikes_sebelumnya,
        LAG(num_scooters_available) OVER w AS scooters_sebelumnya,
        LAG(is_installed)         OVER w AS installed_sebelumnya,
        LAG(is_renting)           OVER w AS renting_sebelumnya,
        LAG(is_returning)         OVER w AS returning_sebelumnya,
        LAG(is_disabled)          OVER w AS disabled_sebelumnya
    FROM gabungan
    WINDOW w AS (PARTITION BY gbfs_station_id ORDER BY snapshot_timestamp)
),

-- ------------------------------------------------------------------
-- Hanya baris dari snapshot baru, dan hanya yang benar-benar berubah.
--
-- Baris benih dibuang di sini: ia hanya dipakai agar LAG punya
-- pendahulu, bukan untuk ikut tersimpan.
-- ------------------------------------------------------------------
perubahan AS (
    SELECT *
    FROM berurutan
    WHERE _baru
      AND (
            bikes_sebelumnya IS NULL
         OR num_bikes_available     IS DISTINCT FROM bikes_sebelumnya
         OR num_docks_available     IS DISTINCT FROM docks_sebelumnya
         OR num_ebikes_available    IS DISTINCT FROM ebikes_sebelumnya
         OR num_scooters_available  IS DISTINCT FROM scooters_sebelumnya
         OR is_installed            IS DISTINCT FROM installed_sebelumnya
         OR is_renting              IS DISTINCT FROM renting_sebelumnya
         OR is_returning            IS DISTINCT FROM returning_sebelumnya
         OR is_disabled             IS DISTINCT FROM disabled_sebelumnya
      )
)

SELECT
    station_key,
    gbfs_station_id,
    legacy_station_id,
    station_name,

    snapshot_timestamp                            AS valid_from,
    CAST(snapshot_timestamp AS DATE)              AS snapshot_date,

    -- 'c' = kemunculan pertama yang tercatat, 'u' = berubah.
    -- Mengikuti konvensi Debezium agar maknanya langsung dikenali.
    CASE WHEN bikes_sebelumnya IS NULL THEN 'c' ELSE 'u' END AS op,

    -- Nilai sebelum & sesudah untuk metrik utama. `bikes_before` NULL
    -- hanya pada op='c' — tidak ada keadaan sebelumnya untuk dibandingkan.
    bikes_sebelumnya                              AS bikes_before,
    num_bikes_available                           AS bikes_after,
    docks_sebelumnya                              AS docks_before,
    num_docks_available                           AS docks_after,

    -- Selisih bertanda: positif berarti sepeda bertambah.
    num_bikes_available - bikes_sebelumnya        AS delta_bikes,
    num_docks_available - docks_sebelumnya        AS delta_docks,

    -- Keadaan setelah perubahan, untuk analisis hunian & durasi.
    num_ebikes_available,
    num_scooters_available,
    is_installed,
    is_renting,
    is_returning,
    is_disabled,

    risk_level,
    is_operational,
    is_unknown_station,
    last_reported,
    report_age_seconds
FROM perubahan
