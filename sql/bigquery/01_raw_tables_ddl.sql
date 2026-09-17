-- ============================================================
-- DDL Raw Layer — BigQuery
--
-- Dijalankan oleh infra/bigquery/setup.sh dengan substitusi variabel dari .env
-- (GCP_PROJECT_ID, BQ_DATASET_RAW).
--
-- Catatan pengembangan:
--
-- - Semua tabel fact di-PARTITION dan di-CLUSTER by kolom yang paling sering
--   dipakai filter/join.
--
-- - Partisi memakai kolom metadata DATE eksplisit, bukan DATE(started_at), agar
--   pengelolaan per partisi bisa tepat sasaran.
--
-- - Idempotensi load lewat delete-partition lalu append
--   (dags/common/bq_utils.py), BUKAN partition decorator: decorator tidak
--   didukung untuk tabel ber-partisi kolom.
--
-- - Skema raw sengaja permisif (boleh NULL) karena validasi bisnis terjadi di
--   layer staging dbt.
-- ============================================================

-- ------------------------------------------------------------
-- 1. Trip history (batch) — 1 baris per trip
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_RAW}.trips`
(
  ride_id            STRING    NOT NULL OPTIONS (description = 'ID unik per trip (natural key dari sumber)'),
  rideable_type      STRING             OPTIONS (description = 'classic_bike | electric_bike'),
  started_at         TIMESTAMP          OPTIONS (description = 'Waktu mulai trip (waktu lokal NYC)'),
  ended_at           TIMESTAMP          OPTIONS (description = 'Waktu selesai trip'),
  start_station_name STRING,
  start_station_id   STRING,
  end_station_name   STRING,
  end_station_id     STRING,
  start_lat          FLOAT64,
  start_lng          FLOAT64,
  end_lat            FLOAT64,
  end_lng            FLOAT64,
  member_casual      STRING             OPTIONS (description = 'member | casual'),

  -- Metadata ingestion (diisi DAG, bukan dari sumber)
  _source_file       STRING             OPTIONS (description = 'Nama file CSV asal (audit trail)'),
  _ingested_at       TIMESTAMP          OPTIONS (description = 'Waktu load ke BigQuery'),
  _ride_started_date DATE               OPTIONS (description = 'Kunci partisi = DATE(started_at)')
)
PARTITION BY _ride_started_date
CLUSTER BY start_station_id, end_station_id
OPTIONS (
  description = 'Raw trip history Citi Bike (batch). 1 baris = 1 trip. Tidak ada transformasi bisnis di layer ini.'
);

-- ------------------------------------------------------------
-- 2. Station status (streaming, periodic snapshot)
--    1 baris per stasiun per polling
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_RAW}.station_status`
(
  station_id              STRING    NOT NULL OPTIONS (description = 'ID stasiun (join ke station_information)'),
  num_bikes_available     INT64,
  num_ebikes_available    INT64,
  num_scooters_available  INT64,
  num_docks_available     INT64,
  num_bikes_disabled      INT64,
  num_docks_disabled      INT64,
  is_installed            BOOL,
  is_renting              BOOL,
  is_returning            BOOL,
  is_disabled             BOOL,
  last_reported           TIMESTAMP          OPTIONS (description = 'Waktu laporan dari operator (dasar freshness check)'),
  snapshot_timestamp      TIMESTAMP          OPTIONS (description = 'Waktu polling producer'),

  -- Metadata ingestion
  _ingested_at            TIMESTAMP,
  _snapshot_date          DATE               OPTIONS (description = 'Kunci partisi = DATE(snapshot_timestamp)')
)
PARTITION BY _snapshot_date
CLUSTER BY station_id
OPTIONS (
  description = 'Raw GBFS station_status (streaming periodic snapshot). 1 baris = 1 stasiun x 1 polling.'
);

-- ------------------------------------------------------------
-- 3. Dead Letter Queue (payload gagal di-parse)
--    Prinsip "never silently drop data"
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_RAW}.station_status_dlq`
(
  raw_payload     STRING    OPTIONS (description = 'Payload mentah yang gagal di-parse'),
  error_reason    STRING    OPTIONS (description = 'Alasan teknis kegagalan (schema/format)'),
  kafka_topic     STRING,
  kafka_partition INT64,
  kafka_offset    INT64,
  failed_at       TIMESTAMP OPTIONS (description = 'Waktu kegagalan terdeteksi')
)
PARTITION BY DATE(failed_at)
OPTIONS (
  description = 'Dead Letter Queue untuk payload station_status yang tidak bisa di-parse.'
);

-- ------------------------------------------------------------
-- 4. Station information (referensi semi-statis)
--    Sumber utama dim_station
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_RAW}.station_information`
(
  station_id        STRING    NOT NULL,
  name              STRING,
  short_name        STRING,
  lat               FLOAT64,
  lon               FLOAT64,
  capacity          INT64,
  region_id         STRING,
  rental_methods    ARRAY<STRING>,
  eightd_has_key_dispenser BOOL,
  _ingested_at      TIMESTAMP
)
OPTIONS (
  description = 'GBFS station_information — snapshot referensi stasiun (kapasitas & koordinat).'
);

-- ------------------------------------------------------------
-- 5. (Opsional) Tabel metrik kualitas data
--    Menampung jumlah baris valid/rejected per batch & polling,
--    dipakai untuk trigger alert lonjakan karantina
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_RAW}.dq_metrics`
(
  run_date          DATE,
  layer             STRING   OPTIONS (description = 'trips | station_status'),
  run_id            STRING   OPTIONS (description = 'ID batch/polling'),
  total_rows        INT64,
  valid_rows        INT64,
  rejected_rows     INT64,
  duplicate_rows    INT64,
  rejected_pct      FLOAT64,
  noted_at          TIMESTAMP
)
PARTITION BY run_date
CLUSTER BY layer
OPTIONS (
  description = 'Metrik DQ per batch/polling. Dasar deteksi lonjakan data karantina (>5%).'
);
