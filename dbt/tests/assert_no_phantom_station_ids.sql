-- ============================================================
-- Uji singular: tidak ada lagi station_id "phantom"
--
-- Phantom = dua station_id berbeda yang menunjuk stasiun fisik yang
-- SAMA (nama identik + koordinat identik). Kasus nyata:
--
--     5343.1   Allen St & Hester St   lat=40.71606 lng=-73.99191
--     5343.10  Allen St & Hester St   lat=40.71606 lng=-73.99191
--
-- Sebelum perbaikan: 65 pasangan seperti ini membuat satu stasiun
-- tampil sebagai dua baris di dashboard dan mencampur analisis
-- net_flow (5343.1 net -4.645 dan 5343.10 net +4.536 terlihat
-- "seimbang" padahal gabungannya tidak).
--
-- Uji gagal bila query mengembalikan baris (ada kelompok dengan >1 id).
--
-- CATATAN: stasiun yang memang terpisah secara fisik TIDAK terpengaruh
-- karena koordinatnya berbeda, mis.:
--     7625.18 vs 7625.22  (E 118 St & Park Ave)
--     8381.04 vs 8421.03  (W 181 St & Riverside Dr)
-- ============================================================

SELECT
    station_name,
    ROUND(lat, 6)                     AS lat,
    ROUND(lng, 6)                     AS lng,
    COUNT(DISTINCT station_id)        AS n_station_ids,
    STRING_AGG(DISTINCT station_id, ' | ' ORDER BY station_id) AS station_ids
FROM {{ ref('dim_station') }}
WHERE station_name IS NOT NULL
  AND lat IS NOT NULL
  AND lng IS NOT NULL
GROUP BY station_name, lat, lng
HAVING COUNT(DISTINCT station_id) > 1
