{#
    Hari-dalam-minggu versi ISO: 1 = Senin ... 7 = Minggu.

    BigQuery TIDAK mengenal date part `ISODAYOFWEEK`:
        EXTRACT(ISODAYOFWEEK FROM d)
        -> "A valid date part name is required but found ISODAYOFWEEK"

    Yang tersedia hanya `DAYOFWEEK` dengan basis MINGGU (1 = Minggu ...
    7 = Sabtu). Konversi ke basis ISO:

        ISO = MOD(DAYOFWEEK + 5, 7) + 1

    Contoh: Senin (DAYOFWEEK=2) -> MOD(7,7)+1 = 1
            Minggu (DAYOFWEEK=1) -> MOD(6,7)+1 = 7

    Dipakai oleh dim_date dan fct_trips supaya keduanya tidak mungkin
    berbeda tafsir.
#}
{% macro iso_day_of_week(date_expr) -%}
MOD(EXTRACT(DAYOFWEEK FROM {{ date_expr }}) + 5, 7) + 1
{%- endmacro %}
