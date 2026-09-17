"""Kode pipeline streaming Citi Bike (GBFS station_status).

Berisi producer (poll GBFS -> Kafka) dan consumer (Kafka -> datalake & warehouse).

Paket ini juga dipakai dari sisi Airflow: DAG `citibike_ingest_station_info`
mengimpor `streaming.schemas` untuk validasi payload GBFS, sehingga definisi
skema cukup ada di satu tempat.
"""
