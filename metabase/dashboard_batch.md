# Dashboard Batch (Metabase) — 4 Chart v1

Resep langkah demi langkah untuk chart **batch** (chart 1–4).
Chart streaming (5–7) dibahas terpisah di
[`dashboard_streaming.md`](dashboard_streaming.md).

**Sumber data:** dataset `shuhaib_citibike_dashboard` (hasil dbt).
Semua mart adalah **view**, jadi chart selalu menampilkan data terbaru
setelah `dbt run`.

---

## Persiapan: konfigurasi database

1. Admin → **Databases** → **Add a database** → **BigQuery**
2. Isi:
   - **Display name**: `Citibike (Batch)`
   - **Project ID**: `jcdeah-009`
   - **Dataset ID**: `shuhaib_citibike_dashboard`
   - **Service account JSON**: tempel **seluruh isi** `secrets/service-account.json`
3. **Save** → **Sync database schema**
4. Pastikan muncul 4 tabel: `trip_summary_daily`, `station_popularity`,
   `usage_pattern_hourly`, `member_vs_casual_behavior`

> Untuk chart streaming nanti, buat koneksi kedua dengan Dataset ID yang sama
> agar auto-refresh-nya bisa diatur terpisah.

---

## Chart 1 — Line: Total Trips per Hari

**Tujuan bisnis:** melihat tren demand harian; dasar keputusan alokasi
tenaga rebalancing per hari.

| Item | Nilai |
|---|---|
| Mart | `trip_summary_daily` |
| Visualisasi | **Line** |
| X-axis | `date_day` |
| Y-axis | `total_trips` |
| Y-axis (opsional) | `member_trips`, `casual_trips` → **Add series** |

**Query (native):**

```sql
SELECT
    date_day,
    total_trips,
    member_trips,
    casual_trips,
    avg_duration_minutes
FROM `jcdeah-009.shuhaib_citibike_dashboard.trip_summary_daily`
ORDER BY date_day
```

**Angka yang diharapkan** (patokan verifikasi):
- Rentang: **2025-12-31 → 2026-03-31** (90 hari)
- Total: **5.952.072** trip · rata-rata **66.134/hari**
- Puncak: **154.162** trip pada 2026-03-31 (Selasa)

> Jika angka jauh berbeda, mart belum ter-sync — jalankan ulang `dbt run`.

---

## Chart 2 — Bar: Top 10 Stasiun Tersibuk

**Tujuan bisnis:** stasiun dengan aktivitas tertinggi = prioritas pengawasan
ketersediaan sepeda.

| Item | Nilai |
|---|---|
| Mart | `station_popularity` |
| Visualisasi | **Bar** (horizontal) |
| X-axis | `total_activity` |
| Y-axis | `station_name` |
| Filter | `activity_rank <= 10` |
| Sort | `total_activity` **descending** |

**Query (native):**

```sql
SELECT
    activity_rank,
    station_name,
    departure_count,
    arrival_count,
    total_activity,
    net_flow
FROM `jcdeah-009.shuhaib_citibike_dashboard.station_popularity`
ORDER BY activity_rank
LIMIT 10
```

**Angka yang diharapkan:**

| Rank | Stasiun | Aktivitas | net_flow |
|---|---|---|---|
| 1 | W 21 St & 6 Ave | 48.167 | −95 |
| 2 | Pier 61 at Chelsea Piers | 46.627 | −99 |
| 3 | Lafayette St & E 8 St | 40.605 | −113 |
| 4 | W 31 St & 7 Ave | 39.135 | −71 |
| 5 | 9 Ave & W 33 St | 38.494 | −126 |
| 6 | Broadway & E 14 St | 35.848 | −126 |
| 7 | E 17 St & Broadway | 34.948 | −164 |
| 8 | 11 Ave & W 41 St | 34.936 | −188 |
| 9 | Norfolk St & Broome St | 33.346 | −142 |
| 10 | Broadway & W 58 St | 32.485 | +231 |

> Semuanya area transit/komuter Manhattan — konsisten dengan problem
> statement (stasiun perkantoran padat di jam sibuk).

**Kolom `net_flow`** = keberangkatan − kedatangan:
- `net_flow` **positif besar** → stasiun cenderung **kehabisan** sepeda
  (banyak orang berangkat dari sini) → kandidat **pengiriman** sepeda
- `net_flow` **negatif besar** → stasiun cenderung **penuh**
  (banyak orang mengembalikan sepeda) → kandidat **penarikan** sepeda

Tampilkan `net_flow` sebagai kolom tambahan di tabel — ini jembatan ke
chart 6 (risk monitoring) di dashboard streaming.

### Kandidat rebalancing (net_flow ekstrem)

Chart terpisah yang sangat berguna: 5 stasiun dengan net_flow paling
positif dan paling negatif. Inilah daftar aksi untuk tim ops.

```sql
-- Paling positif: cenderung KEHABISAN sepeda → kirim sepeda ke sini
SELECT station_name, total_activity, net_flow, activity_rank
FROM `jcdeah-009.shuhaib_citibike_dashboard.station_popularity`
ORDER BY net_flow DESC
LIMIT 5
```

| net_flow | Stasiun | Rank aktivitas |
|---|---|---|
| +727 | Eastern Pkwy & Kingston Ave | #525 |
| +642 | Grove St & Broadway | #682 |
| +579 | W 56 St & 10 Ave | #211 |
| +574 | Eastern Pkwy & Nostrand Ave | #938 |
| +564 | Broadway & Kosciuszko St | #990 |

| net_flow | Stasiun | Rank aktivitas |
|---|---|---|
| −460 | North Moore St & Greenwich St | #92 |
| −337 | Washington Pl & Broadway | #323 |
| −246 | Grand St & Elizabeth St | #36 |
| −245 | Washington St & Barrow St | #530 |
| −233 | 8 Ave & W 52 St | #135 |

**Insight penting:** stasiun dengan net_flow ekstrem **belum tentu** stasiun
tersibuk (mis. Eastern Pkwy & Kingston Ave hanya rank #525). Artinya
ketidakseimbangan **tidak bisa** ditebak dari popularitas saja — inilah alasan
tim ops butuh metrik net_flow yang dihitung otomatis.

> **Catatan perbaikan data:** sebelum normalisasi `station_id`, nilai
> net_flow ekstrem mencapai ±4.500 karena satu stasiun tercatat sebagai dua
> id (mis. `5343.1` net −4.645 dan `5343.10` net +4.536). Setelah
> penggabungan, sinyal yang tersisa (+727 / −460) adalah ketidakseimbangan
> **nyata**. Lihat `stg_station_id_mapping`.

---

## Chart 3 — Heatmap: Pola Jam × Hari

**Tujuan bisnis:** membuktikan pola jam sibuk komuter; dasar penjadwalan
rebalancing **proaktif** (sebelum jam sibuk, bukan sesudah).

### ⚠️ Batasan Metabase: Pivot Table tidak bisa dari Native SQL

Pesan ini akan muncul bila memilih Pivot Table pada question Native SQL:

> *"Pivot tables are only supported for questions built in the query builder."*

Penyebabnya: Pivot Table butuh metadata kolom yang disediakan query builder,
sedangkan Native SQL hanya mengembalikan kolom mentah. **Bukan error data** —
querynya tetap benar; hanya visualisasinya yang tidak tersedia.

### Pilihan pendekatan

| Opsi | Cara | Visualisasi tersedia |
|---|---|---|
| **A. Model → query builder** ✅ direkomendasikan | Simpan SQL sebagai **Model**, lalu buat question GUI di atasnya | **Pivot Table**, Heatmap, Bar, Line |
| **B. Native SQL bentuk matriks** | SQL mengembalikan 24 baris × 7 kolom hari | Table (+conditional formatting), Bar |
| **C. Heatmap native** | Cek dulu apakah versi Metabase-mu punya opsi *Heatmap* | Heatmap |

---

### Opsi A (direkomendasikan) — Model + query builder → Pivot Table

1. **New** → **SQL query** → tempel SQL di bawah
2. Saat menyimpan, pilih **Save as → Model**, nama: `usage pattern hourly (model)`
3. **New** → **Question** → **Pick your starting data** → pilih model tadi
4. Di query builder:
   - **Summarize** → **Sum of** `total_trips`
   - **Group by** → `start_hour`, lalu **Add grouping** → `day_name`
5. **Visualization** → **Pivot Table** → sekarang tersedia ✅
6. Pivot Table settings:
   - **Rows**: `start_hour`
   - **Columns**: `day_name`
   - **Values**: `sum`
   - Aktifkan **Conditional Formatting** → *Color scale* agar terbaca seperti heatmap

```sql
-- Query untuk disimpan sebagai Model
SELECT
    day_of_week,
    day_name,
    start_hour,
    total_trips,
    member_trips,
    casual_trips,
    share_of_total_pct
FROM `jcdeah-009.shuhaib_citibike_dashboard.usage_pattern_hourly`
ORDER BY day_of_week, start_hour
```

> Keuntungan Opsi A: pivotnya bisa di-*drill-down* (klik sel untuk lihat
> baris detail) dan pengguna dashboard bisa mengubah grouping sendiri.

---

### Opsi B — Native SQL berbentuk matriks (tanpa pivot)

SQL ini langsung mengembalikan bentuk heatmap: 1 baris per jam,
1 kolom per hari. Cocok untuk **Table** dengan conditional formatting.

```sql
SELECT
    start_hour,
    SUM(IF(day_of_week = 1, total_trips, 0)) AS mon,
    SUM(IF(day_of_week = 2, total_trips, 0)) AS tue,
    SUM(IF(day_of_week = 3, total_trips, 0)) AS wed,
    SUM(IF(day_of_week = 4, total_trips, 0)) AS thu,
    SUM(IF(day_of_week = 5, total_trips, 0)) AS fri,
    SUM(IF(day_of_week = 6, total_trips, 0)) AS sat,
    SUM(IF(day_of_week = 7, total_trips, 0)) AS sun,
    SUM(total_trips)                         AS total_semua_hari
FROM `jcdeah-009.shuhaib_citibike_dashboard.usage_pattern_hourly`
GROUP BY start_hour
ORDER BY start_hour
```

Ganti label kolom jadi nama hari di **Visualization → Column title**
(mis. `mon` → `Senin`). Alias SQL sengaja ASCII karena identifier
non-ASCII bisa bermasalah di BigQuery.

**Verifikasi:** kolom `total_semua_hari` pada baris `start_hour = 17`
harus bernilai **569.555**, dan baris `start_hour = 8` bernilai **412.752**.

---

### Opsi C — Heatmap native (bergantung versi)

Beberapa versi Metabase (v52 ke atas) menyediakan visualisasi **Heatmap**
yang menginginkan bentuk *long* (x, y, value). Bila tersedia:

```sql
SELECT
    day_name,
    start_hour,
    SUM(total_trips) AS total_trips
FROM `jcdeah-009.shuhaib_citibike_dashboard.usage_pattern_hourly`
GROUP BY day_name, start_hour
ORDER BY day_name, start_hour
```

Lalu **Visualization → Heatmap** dengan X = `start_hour`, Y = `day_name`,
Value = `total_trips`.

---

### Angka yang diharapkan (semua opsi)

| Jam | Total trip (3 bulan) |
|---|---|
| 17:00 | 569.555 |
| 18:00 | 511.000 |
| 16:00 | 460.491 |
| **08:00** | **412.752** |
| 15:00 | 411.431 |

> Dua puncak — sore (16–18) dan pagi (08) — adalah pola **commuter**
> klasik: berangkat kerja pagi, pulang sore. Ini bukti kuantitatif untuk
> problem statement proyek ini.

---

## Chart 4 — Donut/Bar: Member vs Casual

**Tujuan bisnis:** memahami profil pengguna; menentukan segmen mana yang
paling terdampak bila stasiun kosong/penuh.

| Item | Nilai |
|---|---|
| Mart | `member_vs_casual_behavior` |
| Visualisasi | **Donut** (untuk pangsa) + **Table** (untuk detail) |
| Dimension | `member_casual` |
| Metric | `total_trips` (donut) |

**Query (native):**

```sql
SELECT
    member_casual,
    description,
    total_trips,
    share_of_total_pct,
    avg_duration_minutes,
    median_duration_minutes,
    avg_distance_km,
    ebike_share_pct,
    weekend_share_pct,
    morning_peak_trips,
    evening_peak_trips
FROM `jcdeah-009.shuhaib_citibike_dashboard.member_vs_casual_behavior`
ORDER BY total_trips DESC
```

**Angka yang diharapkan:**

| Segmen | Trip | Pangsa | Durasi rata² | Jarak rata² | e-bike | Akhir pekan |
|---|---|---|---|---|---|---|
| member | 5.279.590 | 88,70% | 10,45 mnt | 1,81 km | 71,2% | 24,0% |
| casual | 672.482 | 11,30% | 16,60 mnt | 2,05 km | 79,2% | 34,9% |

**Insight:** casual **60% lebih lama** per trip (16,6 vs 10,45 menit) dan porsi
akhir pekannya lebih besar (34,9% vs 24,0%) — konsisten dengan profil
wisatawan/rekreasi, sementara member adalah komuter rutin. Konsekuensinya:
member lebih sensitif terhadap stasiun kosong saat jam sibuk, casual lebih
sensitif saat akhir pekan.

---

## Menyusun Dashboard

1. **New** → **Dashboard** → nama: `Citi Bike — Operasional (Batch)`
2. Tambahkan ADD **question** yang sudah dibuat (chart 1–4)
3. Susun tata letak:
   - Baris 1: **Chart 1** (line, lebar penuh) — tren
   - Baris 2: **Chart 4** (donut) + **Chart 2** (bar)
   - Baris 3: **Chart 3** (pivot/heatmap, lebar penuh)
4. Tambahkan **Text card** di atas berisi narasi singkat:
   > *"Dashboard memantau pola permintaan sepeda untuk mendukung keputusan
   > rebalancing. Data batch (Jan–Mar 2026, 5,95 juta trip valid)."*
5. **Save**

### Auto-refresh

Chart 1–4 bersumber **batch** → auto-refresh cepat tidak diperlukan
(data hanya berubah setelah `dbt run`). Set **auto-refresh: off** atau 1 jam.

Auto-refresh 1 menit disiapkan untuk dashboard **streaming**, agar perubahan
datanya terlihat saat dashboard dibuka.

Keempat mart di sini dimaterialisasi sebagai **table**, bukan view. Alasannya:
sebagai view, tiap kali halaman dibuka berarti memindai ulang `fct_trips`
(5,9 juta baris) hanya untuk menghasilkan 90–2.285 baris. Dibangun sekali sehari
lalu dibaca berulang jauh lebih murah. Rinciannya di
[`../docs/erd.md`](../docs/erd.md) §6.

---

## Troubleshooting

| Gejala | Penyebab | Solusi |
|---|---|---|
| "No tables found" | schema belum di-sync | Admin → Databases → **Sync database schema** |
| Tabel muncul tapi 0 baris | `dbt run` belum dijalankan | Jalankan DAG `citibike_transform_batch` |
| Query error "not found" | Dataset ID salah | Harus `shuhaib_citibike_dashboard` |
| Angka berbeda dari patokan | mart basi | Sync ulang + cek `Total trip = 5.952.072` di chart 1 |
| Koneksi timeout / permission | service account kurang role | Butuh minimal `bigquery.dataViewer` + `bigquery.jobUser` |
