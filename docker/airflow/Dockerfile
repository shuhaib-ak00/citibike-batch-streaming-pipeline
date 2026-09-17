# ============================================================
# Image Airflow + tool transformasi (dbt) + provider GCP.
#
# Build context = ROOT repo (lihat docker-compose.yml), sehingga
# requirements.txt bisa di-copy dari docker/airflow/.
#
# Instalasi memakai file constraints RESMI Airflow supaya seluruh
# dependency (Airflow + provider + dbt) koheren. Ini menghindari
# konflik versi protobuf/grpcio/jsonschema yang umum terjadi.
#
# NOTE migrasi DockerOperator: file requirements sengaja dipisah.
# Image dbt mandiri nanti cukup FROM image ini (tanpa dbt) lalu
# install requirements.txt yang SAMA — tidak ada duplikasi versi.
# ============================================================

ARG AIRFLOW_VERSION=2.9.3
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY --chown=airflow:airflow docker/airflow/requirements.txt /opt/airflow/requirements.txt
USER airflow

RUN pip install --no-cache-dir --retries 5 \
      --constraint "${CONSTRAINT_URL}" \
      -r /opt/airflow/requirements.txt
