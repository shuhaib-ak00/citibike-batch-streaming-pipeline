{#
    Aturan penamaan dataset BigQuery per layer.

    Default dbt menggabungkan schema target dengan custom schema
    ("<BQ_DATASET_DBT>" + "_" + "staging"). Kita buat eksplisit di sini
    supaya nama dataset dijamin stabil dan mudah ditelusuri:

        staging         -> <BQ_DATASET_DBT>_staging
        intermediate    -> <BQ_DATASET_DBT>_intermediate
        marts/core      -> <BQ_DATASET_DBT>_marts
        marts/dashboard -> <BQ_DATASET_DBT>_dashboard

    Contoh bila BQ_DATASET_DBT=shuhaib_citibike:
        shuhaib_citibike_staging, shuhaib_citibike_marts, dst.

    Nilai custom schema datang dari `+schema:` di dbt_project.yml.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
