-- ============================================================
-- assert_station_status_changes_single_create
--
-- Gagalkan bila ada stasiun dengan LEBIH DARI SATU event `op = 'c'`.
-- Ini penjaga utama int_station_status_changes.
--
-- Kegagalan yang ditangkap:
--
--   1. Benih kosong — snapshot batas sudah terhapus retensi, sehingga
--      MAX(valid_from) menunjuk snapshot yang tidak ada di fct_station_status.
--
--   2. Filter incremental salah — batch lama diproses ulang sehingga
--      menghasilkan perubahan "pertama" berulang.
--
--   3. Model dibangun ulang dari nol (--full-refresh) sementara arsip lama
--      belum dibersihkan.
--
-- Arsip ini menyimpan riwayat jangka panjang, jadi kerusakannya baru terasa
-- saat riwayat itu dibutuhkan. Karena itu test ini ada.
--
-- Hasil yang diharapkan: 0 baris.
-- ============================================================

SELECT
    gbfs_station_id,
    COUNTIF(op = 'c') AS jumlah_create
FROM {{ ref('int_station_status_changes') }}
GROUP BY gbfs_station_id
HAVING jumlah_create > 1
