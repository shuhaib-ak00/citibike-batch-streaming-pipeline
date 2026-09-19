# Runbook — Setup & Operasional

Panduan langkah demi langkah menjalankan project dari nol.
Setiap langkah **idempoten** — aman diulang.

---

## 0. Prasyarat

| Kebutuhan | Versi | Cek |
|---|---|---|
| Docker Desktop | ≥ 4.30 | `docker --version` |
| Docker Compose | v2 (bundled) | `docker compose version` |
| Google Cloud SDK | terbaru | `gcloud --version` |
| Python (untuk util lokal) | 3.11+ | `python --version` |

Akun GCP harus aktif dengan **billing enabled** dan $300 free credit.
Pasang budget alert di Billing Console (mis. Rp180.000 & Rp900.000) sebelum
melanjutkan.

---

## 1. Siapkan file `.env`

```bash
cp .env.example .env
```

Lalu isi minimal:

| Variabel | Keterangan |
|---|---|
| `GCP_PROJECT_ID` | ID project GCP kamu |
| `GCS_BUCKET_RAW` | nama bucket, harus **unik global** (mis. `citibike-raw-<namamu>`) |
| `AIRFLOW_FERNET_KEY` | wajib di-generate (lihat bawah) |

Generate Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

> **Windows/Linux saja:** jika `id -u` bukan 50000, set `AIRFLOW_UID` ke
> hasil `id -u` agar file log Airflow tidak dimiliki root.

---

## 2. Setup GCP (sekali saja)

Login & set project aktif:

```bash
gcloud auth login
gcloud config set project "$(grep ^GCP_PROJECT_ID .env | cut -d= -f2)"
```

### 2a. Service account + kunci JSON

```bash
bash infra/gcp/create_service_account.sh
```

Menghasilkan `secrets/service-account.json` dengan role:
`bigquery.dataEditor`, `bigquery.jobUser`, `storage.objectAdmin`.

### 2b. Bucket GCS datalake

```bash
bash infra/gcs/create_buckets.sh
```

Membuat bucket + lifecycle policy (`infra/gcs/lifecycle_policy.json`)
+ placeholder prefix `raw/trips`, `raw/station_status`, dst.

### 2c. BigQuery dataset & tabel raw

```bash
bash infra/bigquery/setup.sh
```

Membuat dataset `${BQ_DATASET_RAW}` (mis. `shuhaib_citibike_raw`) dan 5 tabel
(partition + cluster).
Dataset dbt dibuat otomatis saat `dbt run`.

### Verifikasi cepat

```bash
bq.cmd ls "${GCP_PROJECT_ID}:${BQ_DATASET_RAW}"       # 5 tabel
gcloud.cmd storage ls --recursive "gs://${GCS_BUCKET_RAW}/" | head
```

> **Catatan Windows/Git Bash:** pakai `bq.cmd` / `gcloud.cmd`, bukan `bq` /
> `gcloud`. Wrapper shell milik Cloud SDK gagal di Git Bash dengan
> `python3.12: command not found`.

---

## 3. Jalankan stack Docker

### 3a. Build image (uji paling awal!)

```bash
docker compose build
```

> ⚠️ Langkah ini memvalidasi kombinasi versi `docker/airflow/requirements.txt`
> (dbt + Airflow). Jika gagal karena konflik dependency, sesuaikan pin di
> file itu lalu build ulang.

### 3b. Nyalakan service inti

```bash
docker compose up -d postgres kafka kafka-init airflow-init \
                     airflow-webserver airflow-scheduler \
                     metabase-db metabase
```

Cek status:

```bash
docker compose ps
```

Tunggu hingga `citibike-postgres` dan `citibike-kafka` **healthy**.

### 3c. Akses

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| Metabase | http://localhost:3000 | buat akun saat pertama kali |
| Kafka UI | http://localhost:8081 | tidak ada (terbuka lokal) |

---

## 4. Verifikasi Kafka

```bash
# dari host (listener EXTERNAL di port 29092)
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Harus muncul:

```
citibike.station_status.snapshot
citibike.station_status.dlq
```

### 4a. Verifikasi lewat Kafka UI

Buka **http://localhost:8081** (port diatur `KAFKA_UI_PORT`). Yang bisa
langsung dibaca tanpa perintah CLI:

| Yang dilihat | Di mana | Nilai yang diharapkan |
|---|---|---|
| Broker hidup | **Brokers** | 1 broker, controller aktif (KRaft, tanpa Zookeeper) |
| Jumlah partisi | **Topics** | `snapshot` = **3**, `dlq` = **1** |
| Consumer tertinggal berapa | **Consumers -> `citibike-station-status-writer`** | **lag = 0** saat streaming normal |
| Topik DLQ kosong | **Topics -> dlq -> Messages** | 0 pesan |
| Bentuk payload GBFS | **Topics -> snapshot -> Messages** | JSON berisi `station_id`, `num_bikes_available`, dll |

> **Batasan:** tab **broker metrics** (grafik throughput/CPU) akan tampak
> kosong. Grafik itu butuh JMX exporter yang belum dikonfigurasi, dan
> sengaja tidak ditambahkan karena tidak diperlukan untuk operasi harian.
> Ini bukan kerusakan.

Cara membuktikan UI membaca keadaan **live**, bukan cache — hentikan
**consumer**, bukan producer:

```bash
# Producer tetap memproduksi, consumer berhenti menguras -> lag naik.
docker compose --profile streaming stop streaming-consumer

# Nyalakan lagi -> lag terkuras kembali ke 0.
docker compose --profile streaming start streaming-consumer
```

> Perhatikan arahnya: mematikan **producer** justru **tidak** menaikkan lag,
> karena tidak ada pesan baru yang diproduksi. Yang membuat lag naik adalah
> consumer yang berhenti menguras.
>
> Dua detail hasil pengukuran, supaya tidak dikira UI-nya rusak:
>
> - Setelah 1 siklus (90 detik), lag tepat sebesar jumlah stasiun pada
>   snapshot itu (±2.519) — karena 1 stasiun = 1 pesan.
> - Setelah consumer dinyalakan lagi, lag **belum langsung** turun ke 0.
>   Consumer menahan buffer sampai idle 30 detik (`CONSUMER_FLUSH_SECONDS`)
>   sebelum menulis dan memajukan offset — jadi tunggu ±30-40 detik.

---

## 5. Menjalankan pipeline streaming

```bash
docker compose --profile streaming up -d --build
docker compose logs -f streaming-producer
docker compose logs -f streaming-consumer
```

Stop:

```bash
docker compose --profile streaming down
```

Baca isi topik dari host:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic citibike.station_status.snapshot \
  --max-messages 5
```

---

## 6. Menjalankan dbt

### Dari host (disarankan saat development)

```bash
cd dbt
cp profiles/profiles.yml.example profiles/profiles.yml

# override path kredensial ke file di mesin lokal
export GOOGLE_APPLICATION_CREDENTIALS="$(cd .. && pwd)/secrets/service-account.json"
export GCP_PROJECT_ID=...      # ambil dari .env
export BQ_DATASET_DBT=shuhaib_citibike
export GCP_LOCATION=asia-southeast2

dbt deps
dbt debug
dbt run --select tag:staging
dbt test  --select tag:staging
```

> Jalankan sisa model dalam **satu perintah** (`dbt run --exclude tag:staging`),
> bukan per tag. Grafik ketergantungannya tidak mengikuti urutan layer:
> `int_station_risk_calculation` (intermediate) membutuhkan `dim_station`
> (core) untuk memperoleh kapasitas stasiun, sehingga build per tag akan
> gagal dari nol.

### Dari Airflow

Jalankan DAG `citibike_transform_batch` dari UI. Untuk menjalankan
lengkap lewat CLI:

```bash
docker compose exec -T airflow-scheduler airflow dags test citibike_transform_batch 2026-09-13
```

---

## 7. Operasional harian

| Aksi | Perintah |
|---|---|
| Lihat log service | `docker compose logs -f <service>` |
| Restart Airflow | `docker compose restart airflow-scheduler airflow-webserver` |
| Hentikan semua | `docker compose down` |
| Hentikan + hapus volume | `docker compose down -v` ⚠️ menghapus metadata Airflow & Kafka |
| Rebuild image Airflow | `docker compose build --no-cache airflow-webserver` |

### Daftar DAG dan jadwalnya

| DAG | Jadwal | Peran |
|---|---|---|
| `citibike_ingest_trips` | `@daily` | CSV → parquet di GCS → tabel raw BigQuery |
| `citibike_ingest_station_info` | `@daily` | GBFS station_information → tabel referensi |
| `citibike_transform_batch` | terpicu Dataset `RAW_TRIPS` | dbt: staging → gate DQ → intermediate → marts |
| `citibike_transform_streaming` | tiap jam | dbt: hanya rantai streaming + arsip perubahan |
| `citibike_watchdog_streaming` | tiap 5 menit | kesehatan streaming (freshness, consumer, DLQ, karantina) |
| `citibike_watchdog_pipeline` | tiap 10 menit | ringkasan tiap DAG-run yang gagal |
| `citibike_retention_cleanup` | Minggu 03:00 | hapus data operasional yang melewati masa simpan |

Semua DAG memakai `is_paused_upon_creation=False`. Ini bukan detail gaya:
DAG yang terjadwal lewat **Dataset** akan gagal secara senyap bila ter-pause
— ia tidak terpicu, tanpa error apa pun. Karena itu DAG baru harus dipastikan
unpaused setelah clone segar.

### Lapisan CDC (perubahan stasiun)

Arsip perubahan disimpan di `int_station_status_changes`, dengan validity
window di `int_station_status_windows`. Rincian desainnya di
[`architecture.md`](./architecture.md) §8.

Query dwell time — **berhasil mengukur berapa lama sebuah stasiun bertahan
dalam suatu kondisi**, yang tidak bisa dijawab periodic snapshot murni:

```sql
-- Stasiun mana yang paling lama bertahan kosong / penuh?
-- `is_possible_gap` WAJIB disaring: interval yang terlalu panjang
-- menandakan data tidak terkumpul, bukan stasiun yang benar-benar diam.
SELECT station_name, risk_level, duration_minutes,
       CAST(valid_from AS STRING) AS mulai
FROM `jcdeah-009.shuhaib_citibike_intermediate.int_station_status_windows`
WHERE duration_minutes IS NOT NULL
  AND NOT is_possible_gap
  AND risk_level IN ('empty', 'full')
ORDER BY duration_minutes DESC
LIMIT 10;
```

```sql
-- Stasiun paling fluktuatif (bergerak paling sering).
SELECT gbfs_station_id, COUNT(*) AS jumlah_perubahan,
       SUM(ABS(delta_bikes)) AS total_sepeda_bergerak
FROM `jcdeah-009.shuhaib_citibike_intermediate.int_station_status_changes`
GROUP BY gbfs_station_id
ORDER BY jumlah_perubahan DESC
LIMIT 10;
```

> **Jangan lupa `NOT is_possible_gap`.** Terverifikasi pada data nyata:
> tanpa saringan itu, dwell time terpanjang yang muncul (3.964 menit)
> sepenuhnya artefak — semuanya berawal pada timestamp yang sama, yaitu
> titik streaming berhenti.

### Retention data operasional

Dijalankan otomatis oleh DAG `citibike_retention_cleanup` (Minggu 03:00).
Tidak perlu langkah manual; jalankan ini bila perlu memicu lebih cepat:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger citibike_retention_cleanup
```

Versi manualnya ada di `sql/bigquery/03_retention.sql`. Perhatikan: tabel
karantina dbt **tidak** dibersihkan, karena keduanya view — `DELETE`
terhadapnya gagal dengan "not allowed for this operation because it
currently has type VIEW". Lihat `architecture.md` §6.5.

### Menyesuaikan kepekaan alert

Ambang dan pembatas alert diatur lewat variabel lingkungan, jadi tidak perlu
mengubah kode. Ubah `.env` lalu **recreate** container (`docker compose up -d`,
bukan `restart`) karena variabel lingkungan hanya dibaca saat container dibuat.

| Variabel | Default | Arti |
|---|---|---|
| `STREAMING_FRESHNESS_THRESHOLD_MINUTES` | 10 | umur snapshot dianggap basi |
| `CONSUMER_LIVENESS_MINUTES` | 10 | datalake dianggap berhenti menerima tulisan |
| `DLQ_WINDOW_MINUTES` / `DLQ_ALERT_ROWS` | 30 / 200 | lonjakan payload rusak |
| `STREAMING_REJECTION_THRESHOLD_PCT` | 5 | proporsi karantina streaming |
| `WATCHDOG_FAILED_RUN_LOOKBACK_MINUTES` | 60 | seberapa jauh ke belakang DAG-run gagal dicari |
| `ALERT_DEDUP_WINDOW_SECONDS` | 3600 | alert berulang dengan kunci sama ditahan |
| `ALERT_BURST_MAX_ALERTS` | 5 | batas pesan kegagalan task per menit |

`ALERT_DEDUP_WINDOW_SECONDS` sebaiknya tidak lebih pendek dari
`WATCHDOG_FAILED_RUN_LOOKBACK_MINUTES`, kalau tidak satu DAG-run gagal akan
dilaporkan berulang kali.

### Menguji alert tanpa menunggu kegagalan nyata

```bash
# Paksa freshness dianggap basi, lalu jalankan watchdog streaming.
docker compose exec -T -e STREAMING_FRESHNESS_THRESHOLD_MINUTES=0 \
  airflow-scheduler airflow tasks test citibike_watchdog_streaming check_freshness 2026-09-13

# Laporkan ulang DAG-run yang gagal.
docker compose exec -T airflow-scheduler airflow dags test citibike_watchdog_pipeline 2026-09-13
```

Jalankan perintah yang sama dua kali untuk memverifikasi pembatas dedup:
pada percobaan kedua log akan memuat `ditahan (dedup ...)`.

---

## 8. Troubleshooting

| Gejala | Penyebab umum | Solusi |
|---|---|---|
| `docker compose build` gagal di pip | konflik versi dbt × Airflow | sesuaikan pin di `docker/airflow/requirements.txt` |
| Airflow UI tidak bisa dibuka | `airflow-init` belum selesai | `docker compose logs airflow-init` |
| DAG tidak muncul di UI | error import di `dags/` | `docker compose exec airflow-scheduler airflow dags list-import-errors` |
| DAG terjadwal tidak pernah jalan | DAG ter-pause; DAG ber-jadwal Dataset gagal **tanpa error apa pun** | cek `airflow dags list -o plain`, pastikan `is_paused_upon_creation=False` lalu `airflow dags unpause <dag_id>` |
| Alert terpasang tetapi tidak pernah berbunyi | `on_failure_callback` tingkat DAG gagal karena `NotFullyPopulated` pada DAG ber-dynamic task mapping | jangan pakai callback tingkat DAG; pakai `citibike_watchdog_pipeline` (lihat `architecture.md` §6.3) |
| Alert sama terkirim berkali-kali | kunci dedup berbeda per task | cek `ALERT_BURST_MAX_ALERTS` dan `ALERT_DEDUP_WINDOW_SECONDS` |
| Perubahan `.env` tidak berpengaruh | variabel lingkungan hanya dibaca saat container dibuat | `docker compose up -d` (recreate), bukan `restart` |
| `DELETE` ke tabel dbt gagal: "has type VIEW" | model staging bermaterialisasi view, jadi tidak bisa di-DELETE | hapus perintah DELETE-nya; view tidak menyimpan data |
| Kafka container restart terus | `CLUSTER_ID` bentrok dengan data volume lama | `docker compose down -v` lalu up ulang |
| Consumer gagal auth GCP | `secrets/service-account.json` belum ada | jalankan `infra/gcp/create_service_account.sh` |
| `bq: command not found` | Cloud SDK belum di-PATH | install Cloud SDK, buka terminal baru |
| Bind mount `data/raw` kosong di container | path relatif salah | jalankan compose dari root repo |

---

## 9. Status verifikasi

Potret **2026-09-18** dari project `jcdeah-009`.

`dbt test` **119/119 lulus** (PASS=119, WARN=0, ERROR=0) — dijalankan sebelum
lapisan CDC ditambahkan dan belum diulang sejak data bertambah. Rekonsiliasi
`raw = valid + rejected` dan `fct_trips = valid` **diverifikasi ulang pada
potret ini dan keduanya seimbang**: 5.981.588 = 5.952.072 + 29.516.

Streaming berjalan dengan Kafka lag 0 dan watchdog melaporkan keempat
pemeriksaan sehat — keduanya diukur **saat streaming hidup**. Streaming kini
dimatikan sengaja untuk menekan kuota, jadi angka itu bukan keadaan sekarang.

Watchdog pipeline juga terbukti melaporkan DAG-run gagal beserta daftar
task-nya.

> Angka yang berasal dari streaming (`fct_station_status`, arsip perubahan,
> `dim_date`) bertambah terus dan **tidak boleh dipatok**. Rinciannya di
> [`erd.md`](./erd.md) §8.
