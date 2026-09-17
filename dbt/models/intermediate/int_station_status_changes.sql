-- ============================================================
-- int_station_status_changes — daftar PERUBAHAN nyata per stasiun
--
-- Berbeda dari `fct_station_status` yang menyimpan SELURUH snapshot,
-- model ini hanya menyimpan baris yang benar-benar berubah. Inilah
-- implementasi **snapshot-diff CDC**: perubahan disimpulkan dengan
-- membandingkan dua keadaan berurutan, bukan dibaca dari log perubahan.
--
-- Mengapa snapshot-diff (dan bukan log-based seperti Debezium)
-- ------------------------------------------------------------
-- Sumbernya GBFS REST API yang hanya mengembalikan keadaan penuh, tanpa
-- WAL, tanpa trigger, dan tanpa kolom `updated_at` per catatan. Jadi
-- log-based CDC memang tidak mungkin di sini — snapshot-diff adalah
-- satu-satunya keluarga CDC yang tersedia.
--
-- Yang penting: resolusi waktunya tidak kalah. Karena sumbernya di-poll
-- tiap ~90 detik, semua keluarga CDC hanya bisa mencatat tingkat
-- perubahan yang sama.
--
-- Mengapa incremental
-- -------------------
-- Tujuannya menjadi **arsip jangka panjang**, sehingga harus tetap utuh
-- walau retensi `raw.station_status` dan `fct_station_status` dipangkas.
-- Sebelumnya keduanya menyimpan seluruh riwayat, sehingga tidak ada satu
-- pun model yang menyediakan daftar perubahan.
--
-- Strategi penulisan: insert_overwrite + copy_partitions
-- -----------------------------------------------------
-- Setiap perubahan adalah peristiwa yang tidak pernah berubah, jadi
-- seharusnya hanya ditambahkan.
--
-- Adapter BigQuery **hanya** menerima `merge` dan `insert_overwrite`
-- (terverifikasi dari pesan errornya: "Expected one of: 'merge',
-- 'insert_overwrite'"). Tidak ada strategi `append` seperti di adapter
-- lain, sehingga penghematan tidak bisa didapat dengan cara itu.
--
-- `merge` ditolak karena mahal: terverifikasi di log saat pengujian, tanpa
-- opsi tambahan dbt menjalankan `MERGE (0.0 rows, 130.2 MiB processed)`.
-- Dengan DAG streaming tiap jam, itu ~130 MiB x 720 run ≈ 94 GB kuota
-- query per bulan — hanya untuk menuliskan 0 baris baru.
--
-- `copy_partitions=True` adalah bagian yang **wajib**: tanpa itu,
-- insert_overwrite MENGGANTI seluruh partisi yang tersentuh, sehingga
-- perubahan lain di tanggal yang sama akan terhapus. Dinyalakan, dbt
-- menyalin dulu partisi tujuan yang tumpang tindih sebelum menuliskan
-- yang baru.
--
-- Terverifikasi: dua kali run berturut-turut menghasilkan 60.807 baris
-- (idempoten, tidak ada yang hilang).
--
-- Idempotensinya bergantung pada filter `snapshot_timestamp > batas`,
-- bukan pada kunci unik. Bila filter itu salah, baris akan terduplikasi —
-- dan duplikatnya DITANGKAP oleh test `unique_combination_of_columns`
-- pada (gbfs_station_id, valid_from), bukan dibiarkan senyap.
--
-- Benih (seed): mengapa diperlukan
-- --------------------------------
-- `LAG()` hanya melihat baris yang ada di dalam dataset input. Tanpa
-- benih, baris pertama tiap batch tidak punya pendahulu sehingga
-- dianggap "stasiun baru" — dan setiap rotasi menghasilkan ~2.500 event
-- `op='c'` palsu.
--
-- Benihnya diambil dari **satu snapshot di batas pemrosesan terakhir**,
-- bukan dari baris terakhir per stasiun di tabel ini. Alasannya:
-- seeding per-stasiun akan melewatkan stasiun yang belum pernah berubah
-- (belum punya baris sama sekali), sehingga perubahan pertamanya akan
-- tercatat sebagai `op='c'` padahal stasiunnya sudah lama ada.
--
-- Konsekuensi yang perlu disadari: benih ini membaca `fct_station_status`.
-- Bila snapshot di batas itu sudah terhapus retensi, benih menjadi kosong
-- dan muncul `op='c'` palsu. Itulah yang diperiksa oleh test
-- `assert_station_status_changes_single_create`.
--
-- Mengapa `gbfs_station_id`, bukan `station_key`
-- ---------------------------------------------
-- Kunci diff **wajib** `gbfs_station_id`, karena di `fct_station_status`
-- kolom `station_key` bernilai NULL untuk 82.531 baris (9,9%) — yaitu
-- stasiun yang ada di feed GBFS tetapi tidak punya riwayat trip sehingga
-- tidak masuk `dim_station` (`is_unknown_station = TRUE`).
--
-- Akibatnya sangat serius bila kunci itu dipakai: seluruh 82.531 baris
-- itu masuk ke SATU partisi `LAG()`, sehingga perubahan antar **stasiun
-- yang berbeda** tercampur dan nilai `delta_bikes` sepenuhnya salah.
-- Terverifikasi saat pengujian: 82.183 dari 139.582 baris (59%) rusak
-- karena sebab ini.
--
-- `gbfs_station_id` tidak pernah NULL (terverifikasi: 0 baris) dan
-- berjumlah 2.450 nilai unik — sesuai jumlah stasiun nyata di feed.
-- Ini juga menegaskan peran `dim_station` sebagai jembatan antar ruang id:
-- `station_key` adalah kunci untuk bergabung ke dimensi, sedangkan
-- `gbfs_station_id` adalah identitas stasiun itu sendiri.
--
-- Nilai `station_key` tetap disertakan di keluaran (boleh NULL) agar
-- hasilnya dapat langsung di-join ke `dim_station` bila diperlukan.
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
