{#
    Definisi TUNGGAL "mengapa sebuah trip ditolak".

    Dipakai bersama oleh stg_trips.sql dan stg_trips_rejected.sql supaya
    keduanya tidak mungkin berbeda tafsir. Bila aturan DQ berubah, cukup
    ubah di satu tempat ini.

    Mengembalikan ekspresi STRING: daftar alasan dipisah koma.
    String kosong ('') berarti baris VALID.
#}
{% macro trip_rejection_flags() %}
ARRAY_TO_STRING(
    [
        IF(ride_id IS NULL, 'missing_ride_id', NULL),
        IF(started_at IS NULL, 'missing_started_at', NULL),
        IF(ended_at IS NULL, 'missing_ended_at', NULL),

        IF(ride_id IS NOT NULL AND started_at IS NOT NULL AND ended_at IS NOT NULL
           AND TIMESTAMP_DIFF(ended_at, started_at, SECOND) < 0,
           'negative_duration', NULL),

        IF(ride_id IS NOT NULL AND started_at IS NOT NULL AND ended_at IS NOT NULL
           AND TIMESTAMP_DIFF(ended_at, started_at, SECOND) > {{ var('max_trip_duration_seconds') }},
           'duration_extreme', NULL),

        {# station_id yang isinya nama stasiun (mis. 'Shop Morgan') -> data kotor #}
        IF(start_station_id IS NOT NULL
           AND NOT REGEXP_CONTAINS(start_station_id, r'^([0-9]+(\.[0-9]+)?|(SYS|JC|HB)[0-9]+)$'),
           'invalid_start_station_id', NULL),
        IF(end_station_id IS NOT NULL
           AND NOT REGEXP_CONTAINS(end_station_id, r'^([0-9]+(\.[0-9]+)?|(SYS|JC|HB)[0-9]+)$'),
           'invalid_end_station_id', NULL),

        {# koordinat hilang DAN tidak ada station_id -> tidak bisa dilacak ke stasiun #}
        IF(start_station_id IS NULL AND (start_lat IS NULL OR start_lng IS NULL),
           'missing_start_location', NULL),
        IF(end_station_id IS NULL AND (end_lat IS NULL OR end_lng IS NULL),
           'missing_end_location', NULL),

        {# penjaga anomali geografis — saat ini 0 baris, tetap dijaga #}
        IF(start_lat IS NOT NULL
           AND (start_lat NOT BETWEEN {{ var('nyc_lat_min') }} AND {{ var('nyc_lat_max') }}
             OR start_lng NOT BETWEEN {{ var('nyc_lng_min') }} AND {{ var('nyc_lng_max') }}),
           'start_coord_out_of_nyc', NULL),
        IF(end_lat IS NOT NULL
           AND (end_lat NOT BETWEEN {{ var('nyc_lat_min') }} AND {{ var('nyc_lat_max') }}
             OR end_lng NOT BETWEEN {{ var('nyc_lng_min') }} AND {{ var('nyc_lng_max') }}),
           'end_coord_out_of_nyc', NULL)
    ],
    ','
)
{% endmacro %}
