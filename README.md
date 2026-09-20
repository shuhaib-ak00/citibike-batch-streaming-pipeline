# Citi Bike Data Pipeline

Pipeline data end-to-end (**batch + streaming**) untuk Citi Bike NYC: menyerap
riwayat perjalanan dan status stasiun real-time, mentransformasikannya menjadi
model dimensional di BigQuery, lalu menyajikannya sebagai dashboard operasional.

**Masalah yang diselesaikan.** Stasiun perkantoran kehabisan sepeda pada jam
sibuk pagi, stasiun permukiman kehabisan dock pada jam pulang. Tim ops punya
truk untuk *rebalancing*, tetapi tidak punya cara mengetahui stasiun mana yang
paling mendesak. Pipeline ini menghasilkan daftar prioritas tersebut secara
terus-menerus.

## Tech Stack

| Komponen | Teknologi |
|---|---|
| Cloud / DWH | GCP — GCS (datalake) + BigQuery |
| Orkestrasi | Airflow 2.9.3 (Docker Compose, LocalExecutor) |
| Streaming | Kafka 3.7 (KRaft) — producer poll GBFS, consumer tulis ke GCS/BigQuery |
| Transformasi | dbt 1.8 (staging → intermediate → marts) |
| Dashboard | Metabase |
| Kontainer | Docker Compose |

## Arsitektur

```
            ┌─ CSV trip (bulanan) ──► GCS (Parquet harian) ─┐
Sumber      │                                              ├─► BigQuery raw
            └─ GBFS station_status ─► Kafka ───────────────┘
               station_information
                                                                    │
                                                                    ▼
                                              dbt: staging → intermediate
                                                   → marts/core → marts/dashboard
                                                                    │
                                                                    ▼
                                                            Metabase (8 chart)
```

Empat lapis (medallion): **Raw → Staging → Intermediate → Marts**, dengan marts
terbagi `core` (skema bintang) dan `dashboard` (siap konsumsi BI).

Kedua aliran bertemu di `int_station_demand_vs_supply`, yang mempertemukan
permintaan historis dengan pasokan terkini — itulah yang menghasilkan daftar
prioritas rebalancing.

Rincian lengkap: [`docs/architecture.md`](docs/architecture.md) ·
[`docs/erd.md`](docs/erd.md)

## Struktur Repo

```
dags/          DAG Airflow + helper (common/)
streaming/     Kafka: producer (poll GBFS), consumer (tulis GCS/BigQuery), DLQ
dbt/           Project dbt: models, macros, tests, docs
sql/           DDL BigQuery & query operasional (retention)
infra/         Script setup GCP (service account, bucket, dataset)
docker/        Dockerfile & requirements per image
metabase/      Catatan setup dashboard
notebooks/     Profiling awal & validasi data quality
data/          raw/ (CSV sumber, tidak di-commit) · sample/ · reference/
secrets/       service-account.json (tidak di-commit)
docs/          architecture.md · erd.md · runbook.md
```

## Data

| Sumber | Tipe | Volume |
|---|---|---|
| Trip history (CSV bulanan) | Batch | 5.981.588 trip (Jan–Mar 2026) |
| GBFS `station_status` | Streaming ~90 detik | ~2.450 baris per snapshot (bila penulisannya tidak terpotong) |
| GBFS `station_information` | Referensi harian | 2.520 stasiun |

**Catatan pemilihan data.** Jan–Mar dipilih karena volume terkecil dalam
setahun, sehingga hemat kuota query. Konsekuensinya data berasal dari **musim
dingin**, jadi angka absolut tidak sebanding dengan periode lain — karena itu
mart menyertakan kolom relatif (`activity_rank`, `activity_share`) sebagai
pengganti angka mentah.

Hasil profiling awal: [`data/data_size_citibike.md`](data/data_size_citibike.md)

## Quickstart

Panduan lengkap: **[`docs/runbook.md`](docs/runbook.md)**. Ringkasnya:

```bash
# 1. Konfigurasi
cp .env.example .env      # isi GCP_PROJECT_ID, GCS_BUCKET_RAW, AIRFLOW_FERNET_KEY

# 2. Setup GCP (sekali saja)
bash infra/gcp/create_service_account.sh
bash infra/gcs/create_buckets.sh
bash infra/bigquery/setup.sh

# 3. Jalankan stack
docker compose up -d --build

# 4. Aktifkan aliran streaming (opsional, profil terpisah)
docker compose --profile streaming up -d
```

| Service | URL |
|---|---|
| Airflow | http://localhost:8080 |
| Metabase | http://localhost:3000 |
| Kafka UI | http://localhost:8081 |
| Kafka (dari host) | `localhost:29092` |

## DAG

| DAG | Jadwal | Peran |
|---|---|---|
| `citibike_ingest_trips` | `@daily` | CSV trip → Parquet di GCS → `raw.trips` |
| `citibike_ingest_station_info` | `@daily` | GBFS `station_information` → tabel referensi |
| `citibike_transform_batch` | terpicu Dataset `RAW_TRIPS` | dbt: staging → gate DQ → intermediate → marts |
| `citibike_transform_streaming` | tiap jam | dbt: hanya rantai streaming + arsip perubahan |
| `citibike_watchdog_streaming` | tiap 5 menit | kesehatan streaming (freshness, consumer, DLQ, karantina) |
| `citibike_watchdog_pipeline` | tiap 10 menit | ringkasan tiap DAG-run yang gagal |
| `citibike_retention_cleanup` | Minggu 03:00 | hapus data operasional yang melewati masa simpan |

Enam dari tujuh DAG **menghasilkan atau memelihara** data; tiga di antaranya
hanya *mengamati* dan tidak menyentuh data sama sekali.

Transformasi dipisah dua jadwal karena sumbernya bergerak dengan kecepatan
berbeda: riwayat trip bersifat historis (harian cukup), sedangkan status stasiun
berubah tiap ~90 detik.

### Catatan operasional penting

DAG berjadwal **Dataset** (`transform_batch`) akan gagal secara **senyap** bila
ter-pause: tidak ada error apa pun, ingestion terlihat sukses, tetapi
transformasi tidak pernah berjalan. Karena itu seluruh DAG memakai
`is_paused_upon_creation=False`, dan status pause sebaiknya diverifikasi:

```bash
docker compose exec -T airflow-scheduler airflow dags list -o plain
```

## Model Data

Skema **fact constellation** — dua fact table berbagi dua dimensi:

| Tabel | Grain | Tipe |
|---|---|---|
| `fct_trips` | 1 baris per trip | Transaction fact |
| `fct_station_status` | 1 baris per stasiun per snapshot | Periodic snapshot fact |
| `dim_station` | 1 baris per stasiun | Jembatan dua ruang ID (legacy ↔ GBFS) |
| `dim_date` | 1 baris per tanggal | Dimensi bersama kedua fact |
| `dim_rider_type` | 2 baris | Statis |

Di atasnya ada **lapisan CDC snapshot-diff** (`int_station_status_changes` +
`int_station_status_windows`) yang menyimpan hanya perubahan, sehingga bisa
menjawab *berapa lama sebuah stasiun bertahan dalam kondisi berisiko*.

Materialisasi mengikuti **rasio build terhadap baca**: staging & intermediate
`view` (dibaca jarang), marts `core` & `dashboard` `table` (dibangun sekali,
dibaca berulang oleh BI). Satu pengecualian, `station_volatility`, tetap `view`
agar selalu mengikuti arsip perubahan yang diperbarui tiap jam.

Rincian: [`docs/erd.md`](docs/erd.md)

## Data Quality

Prinsipnya **never silently drop data**: baris yang gagal validasi tidak
dibuang, melainkan dikarantina lengkap dengan alasan penolakannya.

| Mekanisme | Untuk |
|---|---|
| `stg_*_rejected` (karantina) | Baris yang bisa dibaca tetapi melanggar aturan bisnis |
| `station_status_dlq` | Payload yang tidak bisa dibaca (JSON rusak, skema berubah) |

Rekonsiliasi dijaga test otomatis: `raw = valid + rejected` dan
`fct_trips = valid`. Ada juga **gate** di DAG transformasi yang menghentikan
pipeline bila proporsi karantina melewati 5%.

## Monitoring & Alerting

Dua lapis alert, karena kegagalan tunggal dan kegagalan sistemik butuh
perlakuan berbeda:

1. **Per task** — pesan spesifik + tautan log, dedup per `dag+task`.
2. **Per DAG-run** — satu ringkasan berisi daftar task yang gagal, supaya Slack
   tidak dibanjiri saat puluhan task gagal bersamaan.

Ditambah **watchdog streaming** yang memeriksa *gejala*, bukan proses: data
berhenti bertambah terdeteksi tanpa perlu tahu penyebabnya. Ini menutup celah
streaming yang mati tanpa error apa pun.

Ambang dan pembatas alert diatur lewat variabel lingkungan di `.env`
(lihat `.env.example`).

> **Batas yang disadari:** seluruh mekanisme alert berjalan di dalam Airflow,
> sehingga tidak bisa melaporkan bila Airflow sendiri yang mati. Diperlukan
> pemantauan eksternal; keheningan tidak berarti sehat.

## Hasil Verifikasi

Potret **2026-09-20** dari project `jcdeah-009`.

| Metrik | Nilai | Sifat |
|---|---|---|
| Trip di raw layer | 5.981.588 | Tetap |
| Trip valid (`fct_trips`) | 5.952.072 (99,51%) | Tetap |
| Trip dikarantina | 29.516 (0,49%) | Tetap |
| Stasiun (`dim_station`) | 2.285 id kanonik | Tetap |
| `dim_date` | 271 hari (buffer 7 hari) | Bertambah tiap hari |
| `fct_station_status` | 954.345 baris (487 snapshot) | Bertambah saat streaming hidup |
| `int_station_status_changes` | 73.244 baris (7,67% dari fct) | Bertambah saat streaming hidup |
| `dbt test` | **125 lulus, 0 gagal** | Termasuk test mart operasional & volatilitas |
| Kafka consumer lag | 0 | Diukur saat streaming berjalan |
| Watchdog streaming | 4/4 pemeriksaan sehat | Diukur saat streaming berjalan |

> **Kenapa sebagian angka bergerak.** Empat baris teratas berasal dari riwayat
> trip yang sudah final, jadi nilainya tetap. Sisanya berasal dari streaming:
> `fct_station_status` dan arsip perubahan bertambah setiap kali consumer
> menulis snapshot, sedangkan `dim_date` mengikuti tanggal berjalan. Angka di
> tabel ini karena itu **potret satu waktu**, bukan konstanta — jangan dipakai
> sebagai patokan yang harus selalu cocok.
>
> **Streaming sedang dimatikan sengaja** untuk menekan pemakaian kuota GCS &
> BigQuery. Dua baris terakhir merujuk keadaan terakhir saat streaming hidup,
> bukan keadaan sekarang.

## Dokumentasi

| Dokumen | Isi |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Arsitektur: gambaran end-to-end, dua alur, layer data, data quality, pemantauan |
| [`docs/erd.md`](docs/erd.md) | ERD dan penjelasan relasi antar tabel |
| [`docs/runbook.md`](docs/runbook.md) | Setup, operasional harian, troubleshooting |
| [`metabase/README.md`](metabase/README.md) | Cara membangun dashboard |
