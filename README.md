# Citi Bike Data Pipeline & Operational Dashboard

Final project JCDEAH-009 — pipeline **batch + streaming** end-to-end untuk mendukung
keputusan *rebalancing* Citi Bike NYC (stasiun berisiko *empty/full*).

Dokumen acuan: `Project_Brief_CitiBike_Pipeline.md` (brief & desain arsitektur).

## Tech Stack

| Komponen | Teknologi |
|---|---|
| Cloud / DWH | GCP: GCS (datalake) + BigQuery |
| Orchestration batch | Airflow (Docker Compose) |
| Streaming | Kafka (producer poll GBFS → consumer → BigQuery), periodic snapshot |
| Transformasi | dbt (staging → intermediate → marts/core → marts/dashboard) |
| Dashboard | Metabase |
| Kontainer | Docker Compose |

## Struktur Repo

```
dags/          DAG Airflow (ingestion batch, dbt, watchdog streaming & pipeline) + common helpers
streaming/     Kode Kafka: producer (poll GBFS) & consumer (tulis ke BQ/GCS) + DLQ
dbt/           Project dbt: models (staging/intermediate/marts), macros, tests, docs
sql/           DDL BigQuery & query operasional (cleanup, monitoring)
infra/         Script setup GCP (bucket, dataset, service account)
docker/        Dockerfile & requirements per image (airflow, streaming)
metabase/      Catatan setup dashboard
notebooks/     EDA & validasi data quality
data/          raw/ (CSV mentah, tidak di-commit) • sample/ • reference/
secrets/       service-account.json (tidak di-commit)
docs/          architecture.md, runbook.md, screenshots
```

## Data

- Trip history batch: `data/raw/202601..202603-*.csv` (Januari–Maret, dipilih karena
  volume terkecil dalam setahun → hemat biaya query, lihat brief §12).
- Streaming: GBFS `station_status.json` (periodic snapshot, 1 baris/stasiun/polling).

Hasil profiling & temuan DQ: `data/data_size_citibike.md`.

## Quickstart

Panduan lengkap: **[`docs/runbook.md`](docs/runbook.md)**. Ringkasnya:

```bash
# 1. Konfigurasi
cp .env.example .env      # lalu isi GCP_PROJECT_ID, GCS_BUCKET_RAW, AIRFLOW_FERNET_KEY

# 2. Setup GCP (sekali saja)
bash infra/gcp/create_service_account.sh
bash infra/gcs/create_buckets.sh
bash infra/bigquery/setup.sh

# 3. Jalankan stack
docker compose up -d --build
```

| Service | URL |
|---|---|
| Airflow | http://localhost:8080 |
| Metabase | http://localhost:3000 |
| Kafka UI | http://localhost:8081 |
| Kafka (host) | `localhost:29092` |

## Urutan Fase

- [x] Fase A — EDA & profiling (`notebooks/01_eda_trip_history.ipynb`)
- [x] Fase B — skeleton infra: docker-compose, DDL BigQuery, script setup GCP
- [x] Fase C — pipeline batch: Airflow DAG → GCS → BigQuery → dbt → marts
- [x] Fase D — dashboard batch (Metabase)
- [x] Fase E — pipeline streaming: GBFS → Kafka → BigQuery → marts
- [x] Fase F — alert kegagalan pipeline (watchdog streaming, watchdog pipeline, retention)
- [ ] **Fase G — dokumentasi, ERD, slide, simulasi QnA** ← sedang dikerjakan

## DAG

| DAG | Jadwal | Peran |
|---|---|---|
| `citibike_ingest_trips` | `@daily` | CSV trip → parquet di GCS → tabel raw BigQuery |
| `citibike_ingest_station_info` | `@daily` | GBFS station_information → tabel referensi |
| `citibike_transform_batch` | terpicu Dataset `RAW_TRIPS` | dbt: staging → gate DQ → intermediate → marts |
| `citibike_watchdog_streaming` | tiap 5 menit | kesehatan streaming (freshness, consumer, DLQ, karantina) |
| `citibike_watchdog_pipeline` | tiap 10 menit | ringkasan tiap DAG-run yang gagal |
| `citibike_retention_cleanup` | Minggu 03:00 | hapus data operasional yang melewati masa simpan |

## Hasil verifikasi

| Metrik | Nilai |
|---|---|
| Trip di datalake & raw layer | 5.981.588 |
| Trip valid (`stg_trips` → `fct_trips`) | 5.952.072 (99,51%) |
| Trip dikarantina (`stg_trips_rejected`) | 29.516 (0,49%) |
| Stasiun (`dim_station`) | 2.285 id kanonik |
| `dbt test` | 112 lulus, 0 gagal |
| Streaming `station_status` | >400 ribu baris, Kafka lag 0 |
| Watchdog streaming | 4/4 pemeriksaan sehat |
| Watchdog pipeline | terbukti melaporkan DAG-run gagal + daftar task |

Rekonsiliasi terjaga: `raw = valid + rejected` dan `fct_trips = valid`.
