-- ============================================================
-- int_station_status_windows — rentang waktu antar perubahan
--
-- Menambahkan `valid_to` dan durasinya pada setiap perubahan dari
-- `int_station_status_changes`, sehingga terbentuk validity window:
--
--     [valid_from ──────────────── valid_to)
--      keadaan ini bertahan selama itu
--
-- Pertanyaan yang jadi terjawab
-- -----------------------------
-- Inilah alasan model ini ada. Dengan periodic snapshot murni,
-- pertanyaan berikut sulit dijawab karena setiap snapshot hanya
-- menunjukkan keadaan sesaat:
--
--     "Stasiun mana yang SUDAH LAMA kosong?"
--     "Berapa lama sebuah stasiun bertahan dalam kondisi penuh?"
--     "Stasiun mana yang paling sering berubah?"
--
-- Sebelumnya, stasiun yang kosong 3 jam dan yang baru saja kosong
-- tampak sama di dashboard. Padahal bagi tim ops keduanya jauh berbeda.
--
-- Mengapa view, bukan tabel
-- -------------------------
-- Isinya sepenuhnya turunan dari `int_station_status_changes`, tanpa
-- perhitungan tambahan yang mahal. Sebagai view, ia selalu konsisten
-- dengan sumbernya dan tidak perlu dijadwalkan sendiri.
--
-- `valid_to` NULL berarti keadaan itu MASIH berlangsung — belum ada
-- perubahan berikutnya.
--
-- Grain: 1 baris per perubahan per stasiun (= grain sumbernya).
-- ============================================================

{{ config(materialized='view') }}

WITH perubahan AS (
    SELECT * FROM {{ ref('int_station_status_changes') }}
)

SELECT
    *,
    LEAD(valid_from) OVER (
        PARTITION BY gbfs_station_id ORDER BY valid_from
    )                                                AS valid_to,

    -- Diisi hanya bila rentangnya sudah tertutup. NULL berarti keadaan
    -- ini masih berlangsung, sehingga durasinya belum bisa ditentukan.
    TIMESTAMP_DIFF(
        LEAD(valid_from) OVER (PARTITION BY gbfs_station_id ORDER BY valid_from),
        valid_from,
        MINUTE
    )                                                AS duration_minutes,

    -- True bila interval ini terlalu panjang untuk sekadar "tidak ada
    -- perubahan", sehingga kemungkinan besar ADA data yang tidak terkumpul
    -- (streaming mati) di antara kedua perubahan itu.
    --
    -- Ini penting karena keduanya tampak identik di data: "stasiun diam 66
    -- jam" dan "data 66 jam tidak terkumpul" menghasilkan satu baris
    -- interval yang sama. Tanpa penanda ini, analisis dwell time akan
    -- didominasi artefak.
    --
    -- Terverifikasi pada data nyata: 4.821 interval >1000 menit semuanya
    -- berawal pada timestamp yang sama, yaitu titik streaming berhenti.
    COALESCE(
        TIMESTAMP_DIFF(
            LEAD(valid_from) OVER (PARTITION BY gbfs_station_id ORDER BY valid_from),
            valid_from,
            MINUTE
        ) > {{ var('streaming_gap_threshold_minutes', 10) }},
        FALSE
    )                                                AS is_possible_gap
FROM perubahan
