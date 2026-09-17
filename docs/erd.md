# ERD — Entity Relationship Diagram

Skema yang dipakai: **fact constellation** — dua fact table berbagi dua
dimensi.

---

## 1. Ringkasan relasi

```mermaid
erDiagram
    dim_station ||--o{ fct_trips : "start & end_station_key"
    dim_rider_type ||--o{ fct_trips : "rider_type_key"
    dim_date ||--o{ fct_trips : "start_date_key"
    dim_station ||--o{ fct_station_status : "station_key"
    dim_date ||--o{ fct_station_status : "snapshot_date_key"
```

| Fact | Tipe | Grain |
|---|---|---|
| `fct_trips` | Transaction fact | 1 baris per perjalanan |
| `fct_station_status` | Periodic snapshot fact | 1 baris per stasiun per polling |

Perbedaan tipe itu menentukan cara beragregasi: periodic snapshot memakai
`GROUP BY` bucket waktu, bukan validity window.

`dim_rider_type` hanya relevan untuk `fct_trips`, sedangkan `dim_station` dan
`dim_date` dipakai kedua fact.

---

## 2. Detail kolom

```mermaid
erDiagram
    dim_station {
        STRING station_key PK "surrogate"
        STRING station_id UK "ID legacy: 5343.10"
        STRING gbfs_station_id UK "ID GBFS: UUID"
        STRING station_name
        FLOAT64 lat
        FLOAT64 lng
        INT64 capacity "NULL bila GBFS <= 0"
        STRING region_id
        INT64 trip_mentions
        TIMESTAMP first_seen_at
        TIMESTAMP last_seen_at
        BOOL has_capacity_info
        BOOL is_missing_from_gbfs
    }

    dim_date {
        INT64 date_key PK "YYYYMMDD"
        DATE date_day UK
        INT64 year
        INT64 quarter
        INT64 month
        STRING month_name
        INT64 iso_week
        INT64 day_of_week "1=Senin..7=Minggu"
        STRING day_name
        BOOL is_weekend
    }

    dim_rider_type {
        STRING rider_type_key PK
        STRING member_casual UK "member / casual"
        STRING description
    }

    fct_trips {
        STRING trip_key PK
        STRING start_station_key FK
        STRING end_station_key FK
        STRING rider_type_key FK
        INT64 start_date_key FK
        STRING ride_id UK
        TIMESTAMP started_at
        TIMESTAMP ended_at
        DATE trip_date
        INT64 duration_seconds
        FLOAT64 distance_km
        STRING member_casual
    }

    fct_station_status {
        STRING station_status_key PK
        STRING station_key FK "boleh NULL"
        INT64 snapshot_date_key FK
        STRING gbfs_station_id "identitas sebenarnya"
        TIMESTAMP snapshot_timestamp
        DATE snapshot_date
        INT64 num_bikes_available
        INT64 num_docks_available
        INT64 capacity
        FLOAT64 occupancy_rate_pct
        BOOL is_operational
        BOOL is_stale
        BOOL is_unknown_station
        STRING risk_level
    }

    dim_station ||--o{ fct_trips : "start_station_key"
    dim_station ||--o{ fct_trips : "end_station_key"
    dim_rider_type ||--o{ fct_trips : "rider_type_key"
    dim_date ||--o{ fct_trips : "start_date_key"
    dim_station ||--o{ fct_station_status : "station_key"
    dim_date ||--o{ fct_station_status : "snapshot_date_key"
```

---

## 3. Layer raw

```mermaid
erDiagram
    trips {
        STRING ride_id PK
        TIMESTAMP started_at
        TIMESTAMP ended_at
        STRING start_station_id
        STRING end_station_id
        DATE _ride_started_date "partisi"
        STRING _source_file
    }
    station_information {
        STRING station_id PK "ID GBFS"
        STRING short_name "-> ID legacy"
        INT64 capacity
        STRING region_id
    }
    station_status {
        STRING station_id "ID GBFS"
        TIMESTAMP snapshot_timestamp
        INT64 num_bikes_available
        DATE _snapshot_date "partisi"
    }
    station_status_dlq {
        STRING raw_payload
        STRING error_reason
        TIMESTAMP failed_at "partisi"
    }
    dq_metrics {
        DATE run_date "partisi"
        STRING layer
        INT64 total_rows
        INT64 rejected_rows
        FLOAT64 rejected_pct
    }
    station_information ||--o{ station_status : "station_id (ID GBFS)"
```

Catatan: `trips` tidak terhubung langsung ke tabel mana pun di layer raw.
Penghubungnya baru terbentuk di `dim_station` — lihat bagian 4.

---

## 4. `dim_station` sebagai jembatan dua ruang ID

Kedua fact table memakai gaya ID yang berbeda, dan `dim_station` yang
menghubungkannya. Inilah sebabnya dimensi tersebut menyimpan dua kolom ID.

```mermaid
flowchart LR
    T["riwayat trip<br/>(CSV)"] -->|"5343.10, JC116"| F1["fct_trips"]
    G["GBFS<br/>station_status"] -->|"UUID 3bfc859b..."| F2["fct_station_status"]
    F1 -->|"start_station_key"| DS["dim_station"]
    F2 -->|"station_key"| DS
    DS -.->|"short_name<br/>= penghubung"| G
```

**Masalahnya:** dua sumber **tidak saling mengenal**. Join langsung
`fct_trips.start_station_id = fct_station_status.gbfs_station_id`
menghasilkan **0 baris cocok** — bukan sebagian.

**Solusinya:** penghubungnya adalah kolom `short_name` pada GBFS, yang
ironisnya justru berisi ID legacy itu sendiri.

| Kolom | Dipakai oleh | Isi |
|---|---|---|
| `station_id` | `fct_trips` | legacy: `5343.10`, `JC116`, `HB602` |
| `gbfs_station_id` | `fct_station_status` | UUID / snowflake GBFS |

Konsekuensinya: trip dan status stasiun hanya dapat dibandingkan lewat
`dim_station`, tidak pernah secara langsung.

---

## 5. Aliran dari raw ke marts

```mermaid
flowchart TD
    rt["raw.trips"] --> st["stg_trips"]
    rt --> sid["stg_station_id_mapping<br/>(table)"]
    sid --> st
    rt --> str["stg_trips_rejected"]
    rss["raw.station_status"] --> ss["stg_station_status"]
    rss --> ssr["stg_station_status_rejected"]
    st --> ids["int_stations_deduplicated"]
    st --> dq["int_trips_dq_summary"]
    ids --> dsm["dim_station"]
    ri["raw.station_information"] -->|"short_name"| dsm
    st --> ft["fct_trips"]
    dsm --> ft
    ss --> isr["int_station_risk_calculation"]
    dsm --> isr
    isr --> fss["fct_station_status"]
    fss --> ilcs["int_latest_complete_snapshot"]
    fss --> isc["int_station_status_changes<br/>(incremental)"]
    isc --> isw["int_station_status_windows"]
    fss --> marts["marts/dashboard"]
    isd["int_station_demand_vs_supply"] --> marts
    ft --> isd
    isr --> isd
```

`int_station_risk_calculation` (layer intermediate) membaca `dim_station`
(layer core), sehingga eksekusi dbt tidak dapat diurutkan per tag — intermediate
membutuhkan core lebih dulu. Karena itu DAG menjalankan
`dbt run --exclude tag:staging` dalam satu perintah dan membiarkan dbt
menentukan urutannya.

---

## 6. Materialisasi

| Layer | Materialisasi | Alasan |
|---|---|---|
| Staging | **view** | Hemat storage; dihitung saat dipakai |
| Intermediate | **view** | Sama |
| Marts core | **table** | Dipakai berulang; jadi dasar foreign key |
| Marts dashboard | **view** | Selalu segar mengikuti core |

**Pengecualian, beserta alasannya:**

| Model | Materialisasi | Alasan |
|---|---|---|
| `stg_station_id_mapping` | **table** | Dipakai 2× oleh `stg_trips`; kalau view akan memindai raw 1,1 GiB dua kali |
| `station_availability_realtime`, `station_risk_monitoring`, `station_supply_demand` | **table** | Di-auto-refresh tiap menit; kalau view, 1.440 refresh/hari menghabiskan kuota 1 TiB dalam ±3 hari |
| `int_station_status_changes` | **incremental** | Arsip perubahan; harus bertahan walau sumbernya dipangkas retensi |

---

## 7. Penanganan kasus khusus

Tiga hal berikut tidak terlihat di diagram, tetapi menentukan kebenaran relasi
antar tabel.

### 7.1 `capacity` NULL ≠ 0

26 stasiun dilaporkan GBFS berkapasitas **0**, dan satu stasiun
(`E 1 St & Bowery`) berkapasitas **1** padahal rutin menampung puluhan sepeda.

Kalau 0 dipakai apa adanya, `occupancy_rate_pct` menjadi bagi-nol. Karena itu:

```sql
CASE WHEN i.capacity > 0 THEN i.capacity END AS capacity
```

NULL berarti **tidak diketahui**, dan ditandai `has_capacity_info`. Nilai 0 dan
"tidak diketahui" diperlakukan berbeda.

### 7.2 `dim_date` diambil dari gabungan, bukan hanya trip

Kalau rentangnya hanya dari `stg_trips`, fact streaming akan punya tanggal di
luar dimensi → foreign key-nya gagal.

```sql
LEAST(MIN(trip_date), MIN(_snapshot_date))    -- batas bawah dari keduanya
```

Ditambah **buffer 7 hari ke depan**, karena `dim_date` dibangun DAG harian
sementara streaming berjalan tiap jam — tanpa buffer, snapshot lewat tengah
malam belum punya baris di dimensi.

### 7.3 Foreign key boleh NULL, tapi bukan hash dari nilai kosong

```sql
CASE WHEN start_station_id IS NOT NULL
     THEN {{ dbt_utils.generate_surrogate_key(['start_station_id']) }}
END AS start_station_key
```

Nilai NULL, bukan hash dari string kosong. Bila di-hash, kunci itu tidak akan
ditemukan di dimensi dan test `relationships` gagal — sedangkan NULL dilewati
test tersebut, sehingga maknanya terjaga.

Kasus serupa muncul di lapisan delta: `station_key` NULL untuk 82.531 baris
(9,9%), sehingga `int_station_status_changes` memakai `gbfs_station_id` sebagai
kunci perbandingan. Bila `station_key` dipakai, semua baris NULL masuk satu
partisi `LAG()` dan perubahan antar stasiun berbeda tercampur.

---

## 8. Angka nyata

| Entitas | Jumlah baris |
|---|---|
| `raw.trips` | 5.981.588 |
| `stg_trips` (valid) | 5.952.072 |
| `stg_trips_rejected` | 29.516 |
| `dim_station` | **2.285** (2.247 punya capacity, 38 tidak ada di GBFS) |
| `dim_date` | 267 hari (2025-12-31 … 2026-09-23) |
| `dim_rider_type` | 2 |
| `fct_trips` | 5.952.072 |
| `fct_station_status` | 837.683 |
| `int_station_status_changes` | 60.807 (7,26% dari fct) |
| `int_station_status_windows` | 60.807 |

**Rekonsiliasi yang dijaga test:**
`raw = valid + rejected` ✓ · `fct_trips = valid` ✓
