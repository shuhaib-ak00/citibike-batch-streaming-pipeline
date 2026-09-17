# Ukuran & Profil Dataset Citi Bike (Jan–Mar 2026)

> Di-generate otomatis oleh `notebooks/01_eda_trip_history.ipynb`.

## Ringkasan

| Metrik | Nilai |
|---|---|
| Total trip | 5,981,588 |
| Rentang `started_at` | 2025-12-30 23:30:09.507000 s/d 2026-03-31 23:58:13.842000 |
| Trip 2026-01 | 1,816,296 |
| Trip 2026-02 | 1,219,706 |
| Trip 2026-03 | 2,945,320 |
| Rata-rata trip/hari | 65,731 |
| Hari unik | 91 |
| Duplikat `ride_id` | 0 |
| Stasiun unik | 2,356 |

## Temuan DQ (menjadi dasar dbt test di staging)

| Check | Jumlah |
|---|---|
| Durasi negatif | 0 |
| Durasi > 24 jam | 1,956 |
| Start koordinat null | 4,093 |
| End koordinat null | 25,966 |
| Koordinat non-null di luar NYC | 0 |
| station_id format non-standar | 8,186 |
| Nama stasiun dgn spasi di ujung | 507 |
| Trip Des-2025 nyangkut di file Jan | 266 |

## Catatan Desain

- File bulanan Citi Bike dipotong berdasarkan ENDED date (file Mar berisi trip mulai 28 Feb).
- Partisi harian datalake sebaiknya memakai kolom yang konsisten (started date) + dedup ride_id.
- station_id non-standar: suffix '_' (5303.06_ = 5303.06), prefix SYS/JC/HB, dan nama bocor ke kolom id ('Shop Morgan').
- Tidak ada duplikat ride_id di dataset ini.
- Format durasi sudah non-negatif di sumber (min 4 detik).
