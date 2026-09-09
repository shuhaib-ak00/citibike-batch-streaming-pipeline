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
dags/          DAG Airflow (ingestion batch, dbt, watchdog streaming) + common helpers
streaming/     Kode Kafka: producer (poll GBFS) & consumer (tulis ke BQ/GCS) + DLQ
dbt/           Project dbt: models (staging/intermediate/marts), macros, tests, docs
sql/           DDL BigQuery & query operasional (cleanup, monitoring)
infra/         Script setup GCP (bucket, dataset, service account)
docker/        Dockerfile & requirements per image (airflow, streaming)
metabase/      Catatan setup dashboard
notebooks/     EDA & validasi data quality
data/          raw/ (CSV mentah, tidak di-commit) • sample/ (dikomit) • reference/
docs/          architecture.md, erd.md, data dictionary, runbook, screenshots
```

## Data

- Trip history batch: `data/raw/202601..202603-*.csv` (Januari–Maret, dipilih karena
  volume terkecil dalam setahun → hemat biaya query, lihat brief §12).
- Streaming: GBFS `station_status.json` (periodic snapshot, 1 baris/stasiun/polling).

## Cara Menjalankan (menyusul)

Langkah setup lengkap akan diisi di `docs/runbook.md` saat infra selesai.
Urutan kerja mengikuti fase di bawah (lihat juga brief §7–§11).

- [ ] Fase A — EDA & profiling data (notebooks/01_eda_trip_history.ipynb)
- [ ] Fase B — skeleton infra: docker-compose, project GCP, service account
- [ ] Fase C — pipeline batch: Airflow DAG → GCS → BigQuery → dbt → marts
- [ ] Fase D — dashboard batch (Metabase)
- [ ] Fase E — pipeline streaming: GBFS → Kafka → BigQuery → marts
- [ ] Fase F — alert kegagalan pipeline (batch + streaming watchdog)
- [ ] Fase G — dokumentasi, ERD, slide, simulasi QnA
