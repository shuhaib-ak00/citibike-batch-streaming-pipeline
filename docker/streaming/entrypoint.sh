#!/usr/bin/env bash
# ============================================================
# Dispatcher peran container streaming berdasarkan env ROLE.
#
#   ROLE=producer -> streaming/producer/gbfs_poller.py
#   ROLE=consumer -> streaming/consumer/snapshot_writer.py
#
# Kode streaming di-mount ke /opt/app/streaming. Guard di bawah memberi
# pesan jelas bila modulnya belum tersedia, alih-alih traceback Kafka yang
# membingungkan karena modulnya gagal di-import lebih dulu.
# ============================================================
set -euo pipefail

cd /opt/app

case "${ROLE:-}" in
  producer)
    MODULE="streaming.producer.gbfs_poller"
    ;;
  consumer)
    MODULE="streaming.consumer.snapshot_writer"
    ;;
  *)
    echo "[entrypoint] ERROR: ROLE harus 'producer' atau 'consumer' (dapat: '${ROLE:-kosong}')" >&2
    exit 1
    ;;
esac

MODULE_PATH="/opt/app/${MODULE//.//}.py"
if [[ ! -f "${MODULE_PATH}" ]]; then
  echo "[entrypoint] Modul '${MODULE}' tidak ditemukan (${MODULE_PATH})." >&2
  echo "[entrypoint] Pastikan ./streaming ter-mount ke /opt/app/streaming." >&2
  exit 1
fi

echo "[entrypoint] Menjalankan ${MODULE} sebagai ${ROLE}..."
exec python -m "${MODULE}" "$@"
