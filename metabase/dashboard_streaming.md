# Dashboard Streaming (Metabase) — 3 Chart v1

Resep langkah demi langkah untuk chart **streaming**. Melengkapi
[`dashboard_batch.md`](dashboard_batch.md) yang membahas chart batch.

**Sumber data:** dataset `citibike_dashboard` (hasil dbt).
Ketiga mart di sini adalah **tabel** (bukan view), dengan alasan yang dijelaskan
di bagian [Auto-refresh](#auto-refresh-kenapa-murah).

---

## Kenapa chart 5-6 butuh perlakuan berbeda

Chart batch (1-4) aman memakai view: datanya hanya berubah setelah `dbt run`.
Chart streaming sebaliknya — tanpa auto-refresh, tabelnya tampak beku dan tidak
menunjukkan bahwa pipeline-nya hidup. Justru **perubahan angkanya** yang menjadi
bukti aliran streaming berjalan.

Karena itu dua hal disiapkan khusus:

| Kebutuhan | Solusi |
|---|---|
| Auto-refresh tiap menit tapi murah | Mart dimaterialisasi sebagai **table** berisi ±2.500 baris, bukan view yang memindai ±240 MB per refresh |
| Kondisi "terkini" yang tidak menyesatkan | Mart memakai **snapshot terakhir yang lengkap**, bukan snapshot terakhir begitu saja (lihat catatan di bawah) |

> **Catatan penting soal "snapshot terakhir".** Penulisan consumer dapat
> terpotong di tengah sebuah snapshot, sehingga snapshot paling baru bisa
> hanya memuat sebagian kecil stasiun. Mart di sini karena itu memakai
> `int_latest_complete_snapshot`, yang memilih snapshot terakhir dengan
> minimal 95% stasiun. Tanpa itu, peta dan tabel risiko berisiko tampil
> nyaris kosong.

---

## Chart 5 — Pin Map: Status Ketersediaan Live

**Tujuan bisnis:** melihat sebaran spasial stasiun bermasalah sekaligus —
apakah masalahnya mengelompok di satu kawasan atau menyebar.

| Item | Nilai |
|---|---|
| Mart | `station_availability_realtime` |
| Visualisasi | **Map → Pin map** |
| Latitude | `lat` |
| Longitude | `lng` |
| Color / Category | `risk_level` |
| Tooltip | `station_name`, `availability_label`, `num_bikes_available`, `num_docks_available`, `occupancy_rate_pct` |

**Query (native):**

```sql
SELECT
    gbfs_station_id,
    station_name,
    region_id,
    lat,
    lng,
    capacity,
    num_bikes_available,
    num_ebikes_available,
    num_docks_available,
    occupancy_rate_pct,
    risk_level,
    risk_severity,
    availability_label,
    is_operational,
    is_stale,
    last_snapshot_at,
    report_age_seconds
FROM `jcdeah-009.shuhaib_citibike_dashboard.station_availability_realtime`
WHERE lat IS NOT NULL
  AND lng IS NOT NULL
ORDER BY risk_severity DESC
```

Jika peta tampak terlalu padat, tambahkan filter untuk memperjelas tampilan:

```sql
-- hanya stasiun yang butuh perhatian
WHERE risk_level IN ('empty', 'low', 'high', 'full')
```

**Angka yang diharapkan** (patokan verifikasi):

| Label | risk_level | Jumlah | Rata-rata occupancy |
|---|---|---|---|
| Normal | balanced | 1.182 | 49,5% |
| Sepeda sedikit | low | 417 | 10,5% |
| Penuh | full | 320 | 83,6% |
| Kapasitas tidak diketahui | unknown_capacity | 241 | — |
| Dock sedikit | high | 194 | 86,6% |
| Kosong | empty | 68 | 0,0% |
| Tidak beroperasi | not_operational | 27 | 0,2% |

**Pengaturan warna yang disarankan** (Map settings → Marker color):

| risk_level | Warna | Alasan |
|---|---|---|
| `empty` | merah tua | paling mendesak — pengguna tidak bisa mengambil sepeda |
| `low` | oranye | peringatan dini |
| `balanced` | hijau | normal |
| `high` | biru muda | dock mulai terbatas |
| `full` | biru tua | pengguna tidak bisa mengembalikan sepeda |
| `unknown_capacity` | abu-abu | tidak bisa dinilai |
| `not_operational` | abu-abu gelap | di luar lingkup rebalancing |

---

## Chart 6 — Tabel: Stasiun Berisiko Empty/Full

**Tujuan bisnis:** daftar kerja tim ops hari ini — stasiun mana yang perlu
ditangani, dan berapa sepeda harus dipindahkan.

Ini chart paling operasional dari seluruh dashboard. Karena itu mart-nya
sudah disaring: hanya stasiun yang **beroperasi**, datanya **tidak basi**,
dan **punya kapasitas** — tiga syarat agar angkanya bisa ditindaklanjuti.

| Item | Nilai |
|---|---|
| Mart | `station_risk_monitoring` |
| Visualisasi | **Table** |
| Sort | `risk_rank` **ascending** |

**Query (native):**

```sql
SELECT
    risk_rank,
    station_name,
    region_id,
    capacity,
    num_bikes_available,
    num_docks_available,
    ROUND(occupancy_rate_pct, 1)  AS occupancy_pct,
    risk_level,
    recommended_action,
    bikes_to_move,
    last_reported,
    report_age_seconds
FROM `jcdeah-009.shuhaib_citibike_dashboard.station_risk_monitoring`
ORDER BY risk_rank
```

**Angka yang diharapkan:** 999 stasiun berisiko.

| Tindakan | Jumlah stasiun | Total sepeda | Rata-rata per stasiun |
|---|---|---|---|
| Tarik sepeda dari stasiun ini | 514 | 5.142 | 10,0 |
| Kirim sepeda ke stasiun ini | 485 | 7.151 | 14,7 |

**6 baris teratas** (contoh nyata):

| # | Stasiun | Sepeda | Kapasitas | Occupancy | Status | Perlu pindah |
|---|---|---|---|---|---|---|
| 1 | E 40 St & Park Ave | 0 | 123 | 0,0% | empty | 62 |
| 2 | E 24 St & Park Ave S | 0 | 79 | 0,0% | empty | 40 |
| 3 | E 48 St & 5 Ave | 0 | 62 | 0,0% | empty | 31 |
| 4 | E 33 St & 5 Ave | 0 | 60 | 0,0% | empty | 30 |
| 5 | E 53 St & Madison Ave | 0 | 51 | 0,0% | empty | 26 |
| 6 | N 10 St & Berry St | 0 | 47 | 0,0% | empty | 24 |

> **Insight:** lima teratas adalah stasiun di kawasan **perkantoran Midtown
> Manhattan** — persis pola pada problem statement: stasiun perkantoran
> kehabisan sepeda. Inilah alasan tim ops perlu metrik otomatis.

**Format tampilan:** aktifkan *Conditional Formatting → Color scale* pada
`occupancy_pct` agar baris merah untuk kosong dan biru untuk penuh langsung
terlihat tanpa membaca angka.

---

## Chart 7 — Bar: Supply vs Demand per Stasiun

**Tujuan bisnis:** mempertemukan **permintaan historis** dengan **pasokan
terkini** — pertanyaan yang tidak bisa dijawab oleh salah satunya sendiri:

> "Stasiun ini secara historis banyak ditinggalkan, tetapi sekarang sepedanya
> menipis. Berarti ia butuh pengiriman sepeda **sekarang**."

| Item | Nilai |
|---|---|
| Mart | `station_supply_demand` |
| Visualisasi | **Bar** (horizontal) + **Table** untuk detail |

**Query A — stasiun prioritas pengiriman sepeda:**

```sql
SELECT
    station_name,
    net_flow,
    total_activity,
    activity_rank,
    capacity,
    num_bikes_available,
    ROUND(occupancy_rate_pct, 1) AS occupancy_pct,
    risk_level,
    imbalance_direction
FROM `jcdeah-009.shuhaib_citibike_dashboard.station_supply_demand`
WHERE is_priority_supply
ORDER BY net_flow DESC
LIMIT 10
```

**Query B — prioritas penarikan sepeda:**

```sql
SELECT
    station_name,
    net_flow,
    total_activity,
    activity_rank,
    capacity,
    num_docks_available,
    ROUND(occupancy_rate_pct, 1) AS occupancy_pct,
    risk_level,
    imbalance_direction
FROM `jcdeah-009.shuhaib_citibike_dashboard.station_supply_demand`
WHERE is_priority_drain
ORDER BY net_flow ASC
LIMIT 10
```

**Angka yang diharapkan** (2.285 stasiun):

| Arah ketidakseimbangan | Jumlah | Prioritas kirim | Prioritas tarik |
|---|---|---|---|
| Pasokan berlebih, permintaan rendah | 1.141 | 0 | 0 |
| Permintaan tinggi, pasokan cukup | 449 | 0 | 0 |
| Tarik sepeda (pasokan berlebih) | 424 | 0 | **424** |
| Kirim sepeda (permintaan tinggi, pasokan rendah) | 225 | **225** | 0 |
| Seimbang | 46 | 0 | 0 |

**5 teratas prioritas pengiriman:**

| net_flow | Occupancy | Status | Stasiun |
|---|---|---|---|
| +504 | 16,7% | low | Eastern Pkwy & Franklin Av |
| +500 | 0,0% | empty | E 48 St & 5 Ave |
| +472 | 0,0% | empty | President St & Nostrand Av |
| +471 | 11,1% | low | Plaza St East & Flatbush Av |
| +420 | 6,5% | low | Eastern Pkwy & Washington Av |

### ⚠️ Baca kolom peringkat, bukan angka absolut

Data batch berasal dari **Januari-Maret** (musim dingin), sedangkan demo
berlangsung di periode berbeda. Volume trip Jan-Mar (±66 ribu/hari) jauh
berbeda dari periode lain, sehingga **angka absolut tidak sebanding**.

Gunakan kolom relatif:
- `activity_rank` — peringkat kesibukan (1 = tersibuk)
- `activity_share` — pangsa aktivitas (0..1)
- `imbalance_pct` — jarak dari titik seimbang 50%

Cara aman membacanya: bandingkan **peringkat** antar stasiun, bukan
menyimpulkan "stasiun ini ramai" dari `total_activity` mentah.

---

## Auto-refresh: kenapa murah

Ini bagian yang membedakan dashboard streaming dari batch.

| Konfigurasi | Bytes scanned per refresh | 1.440 refresh/hari |
|---|---|---|
| View memindai riwayat (tanpa optimasi) | ±240 MB | ±345 GB/hari → **habis kuota 1 TiB dalam ±3 hari** |
| **Table berisi snapshot terkini** (dipakai) | ±250 KB | ±360 MB/hari → **di bawah 1% kuota** |

Karena itu:

1. **Chart 5 dan 6:** aktifkan auto-refresh **1 menit** karena keduanya
   membaca tabel kecil. Aman dinyalakan terus.
   - Buka dashboard → **...** → **Edit dashboard** → **Auto-refresh → 1 minute**
2. **Chart 7:** sumbernya gabungan batch + streaming tetapi `station_supply_demand`
   juga tabel berisi ±2.285 baris, jadi auto-refresh 1 menit juga aman.
   Namun karena sisi permintaannya statis (batch), auto-refresh **15 menit**
   sudah cukup dan lebih hemat.

> **Kalau nanti mengganti mart streaming menjadi view**, biaya query akan
> melonjak ratusan kali. Perubahan itu harus disertai penyesuaian interval
> auto-refresh.

---

## ⚠️ Jangan pakai AVG untuk mengukur kesegaran data

Perhitungan rata-rata umur data (`report_age_seconds`) **menyesatkan** karena
terdistorsi nilai ekstrem. Terbukti dari data nyata:

| Metrik | Nilai | Artinya |
|---|---|---|
| **Median** | **63 detik** | hampir semua stasiun melapor <1 menit |
| Rata-rata | 14.576 detik (4 jam) | ← salah menggambarkan kenyataan |
| Baris segar (≤10 menit) | 2.432 dari 2.449 (**99,3%**) | |

Penyebab distorsinya: beberapa stasiun non-operasional sudah lama berhenti
melapor — yang terlama **2.589 jam (108 hari)**. Satu nilai ekstrem menarik
rata-rata jauh, sementara mayoritas data justru sangat segar.

**Gunakan median** bila membuat kartu metrik kesegaran:

```sql
SELECT
    COUNT(*)                                                   AS total_stasiun,
    COUNTIF(report_age_seconds <= 600)                         AS segar_10_menit,
    COUNTIF(is_stale)                                          AS basi,
    ROUND(APPROX_QUANTILES(report_age_seconds, 100)[OFFSET(50)]) AS median_umur_detik,
    ROUND(COUNTIF(is_stale) / COUNT(*) * 100, 2)               AS persen_basi
FROM `jcdeah-009.shuhaib_citibike_dashboard.station_availability_realtime`
```

Angka yang diharapkan: `total 2.449`, `segar 2.432`, `basi 17`, `median 63`,
`persen_basi 0,69`.

---

## Menyusun Dashboard Streaming

1. **New → Dashboard**, nama: `Citi Bike — Monitoring Operasional (Streaming)`
2. Tambahkan question chart 5-7
3. Tata letak yang disarankan:
   - Baris 1: **Chart 5** (peta, lebar penuh) — konteks spasial
   - Baris 2: **Chart 6** (tabel risiko, lebar penuh) — daftar aksi
   - Baris 3: Chart 7 query A + query B (dua bar bersebelahan)
4. Tambahkan **Text card** berisi narasi:
   > *"Monitoring near real-time: kondisi ketersediaan sepeda per stasiun.
   > Data diperbarui tiap ±90 detik dari feed GBFS. Stasiun berisiko kosong
   > ditandai merah; tim ops mengirim sepeda ke stasiun tersebut."*
5. Aktifkan auto-refresh 1 menit (lihat bagian Auto-refresh)
6. **Save**

---

## Troubleshooting

| Gejala | Penyebab | Solusi |
|---|---|---|
| Peta kosong atau hanya beberapa pin | Snapshot terakhir tidak lengkap | Pastikan `int_latest_complete_snapshot` terpasang; jalankan `dbt run` |
| Tabel risiko kosong | `is_stale` menyaring semua baris | Cek apakah consumer berhenti — lihat Kafka lag |
| Angka tidak berubah saat auto-refresh | Mart belum di-refresh dbt | Streaming mengisi *raw*; mart perlu `dbt run` untuk memperbarui |
| "Tidak ada stasiun tanpa kapasitas" padahal ada | `capacity` NULL disaring di mart | Memang disengaja — occupancy tidak bisa dihitung tanpa kapasitas |
| Kolom `region_id` kosong di sebagian baris | 250 stasiun tanpa region di feed GBFS | Normal; jangan dipakai sebagai filter wajib |
| Peta padat tak terbaca | 2.449 pin sekaligus | Filter `risk_level IN ('empty','low','high','full')` |

### Mart datar (semua angkanya sama)

Streaming mengisi tabel **raw**, bukan mart. Jadi urutan yang benar:

```
producer/consumer jalan  ->  raw.station_status bertambah
                          ->  dbt run  ->  mart diperbarui  ->  dashboard berubah
```

Kalau mart tidak berubah padahal raw bertambah, artinya `dbt run` belum
dijalankan. Untuk demo, jalankan DAG `citibike_transform_batch` sebelum
presentasi agar angka di dashboard terbaru.
