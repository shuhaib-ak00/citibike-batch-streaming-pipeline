-- ============================================================
-- assert_station_status_changes_single_create
--
-- Gagalkan bila ada stasiun yang punya LEBIH DARI SATU event `op = 'c'`.
--
-- Mengapa test ini penting
-- ------------------------
-- Ini penjaga utama model `int_station_status_changes`. Tanpa benih
-- (seed), `LAG()` tidak melihat keadaan sebelumnya pada baris pertama
-- setiap batch, sehingga baris itu diklasifikasikan sebagai "stasiun
-- baru" — dan setiap rotasi menghasilkan ~2.500 event `'c'` palsu yang
-- mengotori arsip perubahan.
--
-- Kegagalan yang ditangkapnya secara konkret:
--   1. Benih hilang karena snapshot batas sudah terhapus retensi,
--      sehingga `SELECT MAX(valid_from) FROM {{ this }}` menunjuk ke
--      snapshot yang sudah tidak ada di `fct_station_status`.
--   2. Filter incremental salah, sehingga batch lama diproses ulang
--      dan menghasilkan perubahan "pertama" berulang.
--   3. `int_station_status_changes` pernah dibangun ulang dari nol
--      (mis. `--full-refresh`) sementara arsip lama belum dibersihkan.
--
-- Tanpa test ini, arsip bisa rusak tanpa satu pun tanda — dan karena
-- arsip inilah yang menyimpan riwayat jangka panjang, kerusakannya baru
-- terasa saat riwayat itu dibutuhkan.
--
-- Hasil yang diharapkan: 0 baris.
-- ============================================================

SELECT
    gbfs_station_id,
    COUNTIF(op = 'c') AS jumlah_create
FROM {{ ref('int_station_status_changes') }}
GROUP BY gbfs_station_id
HAVING jumlah_create > 1
