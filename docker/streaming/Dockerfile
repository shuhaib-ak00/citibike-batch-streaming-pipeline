# ============================================================
# Image untuk producer & consumer Kafka Citi Bike.
#
# Build context = ROOT repo (lihat docker-compose.yml) sehingga
# requirements.txt bisa di-copy dari docker/streaming/.
#
# Satu image dipakai dua service; peran ditentukan env ROLE
# (producer | consumer) lewat entrypoint.sh.
# ============================================================

FROM python:3.11-slim

# librdkafka sudah dibundel di wheel confluent-kafka, tapi algunas
# dependensi butuh build tools saat resolusi wheel gagal.
ENV PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /opt/app

COPY docker/streaming/requirements.txt /opt/app/requirements.txt
RUN pip install --upgrade pip && pip install -r /opt/app/requirements.txt

COPY docker/streaming/entrypoint.sh /opt/app/entrypoint.sh
RUN chmod +x /opt/app/entrypoint.sh

# Kode streaming di-mount dari ./streaming -> /opt/app/streaming
# (lihat docker-compose.yml) supaya bisa diedit tanpa rebuild.
ENV PYTHONPATH=/opt/app

ENTRYPOINT ["/opt/app/entrypoint.sh"]
