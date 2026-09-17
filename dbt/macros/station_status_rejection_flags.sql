{#
    Aturan TUNGGAL "mengapa satu baris station_status ditolak".

    Dipakai bersama oleh stg_station_status.sql dan
    stg_station_status_rejected.sql sehingga definisi valid di kedua model
    tidak mungkin berbeda tafsir.

    Yang diperiksa di sini hanya hal yang bisa dinilai **tanpa join**:
    kelengkapan field, nilai negatif, dan validitas timestamp. Pemeriksaan
    yang butuh referensi stasiun (kapasitas, keberadaan di dimensi) dilakukan
    di layer intermediate, karena staging tidak boleh bergantung pada mart.

    Mengembalikan ekspresi STRING: daftar alasan dipisah koma.
    String kosong ('') berarti baris VALID.
#}
{% macro station_status_rejection_flags() %}
ARRAY_TO_STRING(
    [
        IF(station_id IS NULL OR station_id = '', 'missing_station_id', NULL),

        IF(num_bikes_available IS NULL, 'missing_bikes_available', NULL),
        IF(num_docks_available IS NULL, 'missing_docks_available', NULL),

        IF(num_bikes_available < 0 OR num_docks_available < 0,
           'negative_availability', NULL),
        IF(COALESCE(num_ebikes_available, 0) < 0
           OR COALESCE(num_bikes_disabled, 0) < 0
           OR COALESCE(num_docks_disabled, 0) < 0,
           'negative_auxiliary_count', NULL),

        {# last_reported adalah dasar freshness check, jadi wajib ada #}
        IF(last_reported IS NULL, 'missing_last_reported', NULL),

        {#
            Sentinel epoch. Feed GBFS mengirim epoch 86400
            (1970-01-02) untuk stasiun yang tidak melaporkan status.
            Nilai ini bukan waktu nyata, sehingga kalau dibiarkan akan
            merusak semua perhitungan umur data.
        #}
        IF(last_reported IS NOT NULL AND last_reported < TIMESTAMP '2000-01-01',
           'invalid_last_reported_sentinel', NULL),

        {# waktu masa depan tidak masuk akal dan menandakan clock/format salah #}
        IF(last_reported IS NOT NULL AND last_reported > TIMESTAMP_ADD(snapshot_timestamp, INTERVAL 5 MINUTE),
           'last_reported_in_future', NULL),

        {# snapshot_timestamp diisi consumer; kosong berarti payload rusak #}
        IF(snapshot_timestamp IS NULL, 'missing_snapshot_timestamp', NULL)
    ],
    ','
)
{% endmacro %}
