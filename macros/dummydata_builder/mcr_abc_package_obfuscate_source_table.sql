{%- macro mcr_abc_package_obfuscate_source_table(source_scheme, source_table_name, date_field='date', limit_rows_by_month=1000, limit_years=2) -%}

{%- if dummydata_mode == false -%}
    select * from {{ source(source_scheme, source_table_name) }}
{%- else -%}

    WITH
    {#  INPUT -#}
    {#  Query gets not dummy date from a source and limit them -#}
    source_not_dummy_data AS (
        select * from {{ source(source_scheme, source_table_name) }}
        where YEAR({{ date_field }}) >= (YEAR({{ date_field }}) - {{ limit_years }})
        limit {{ limit_rows_by_month }} by toStartOfMonth({{ date_field }})
    ),

    {#  SIMPLE OBFUSCATION -#}
    {#  1.3 - is a scale correction value for obfusctate numbers -#}

    {#  Get max date in the source table -#}
    {%- set last_date = mcr_abc_package_get_last_date(source_scheme, source_table_name, date_field) -%}

    {%- set fields_result = mcr_abc_package_get_all_columns_from_source_table(source_scheme, source_table_name) -%}

    source_dummy_data AS (
        SELECT
        {%- for field in fields_result %}
            {{ mcr_abc_package_obfuscate_field(field['name'], field['type'], last_date, 1.3, loop.index) }} AS {{ field['name'] }}
            {%- if not loop.last -%},
            {%- endif -%}
        {%- endfor %} 
        FROM source_not_dummy_data 
    )

    SELECT *
    FROM source_dummy_data

{%- endif -%}

{%- endmacro -%}

{#  MACRO mcr_abc_package_get_all_columns_from_source_table() -#}
{% macro mcr_abc_package_get_all_columns_from_source_table(source_scheme, source_table_name) %}

    {#  Universal way to get all columns with types #}
    {%- set query_get_fields -%}
        DESCRIBE TABLE {{ source(source_scheme, source_table_name) }}
    {%- endset -%}
    {%- set fields_result = run_query(query_get_fields) %}

    {{ return(fields_result) }}

{% endmacro %}

{#  MACRO mcr_abc_package_get_last_date() -#}
{#-  Macro gets the last date in a source. It needs to update date in all rows -#}
    {% macro mcr_abc_package_get_last_date(source_scheme, source_table_name, date_field) %}

    {%- set query_max_date -%}
        select max({{ date_field }}) as max_date
        from {{ source(source_scheme, source_table_name) }}
    {%- endset -%}

    {%- set md_result = run_query(query_max_date) -%}
    {%- if md_result|length -%}
        {%- set last_date = "'" ~ md_result.rows[0]['max_date'] ~ "'" -%}
    {%- else -%}
        {%- set last_date = 'today()' -%}
    {%- endif -%}

    {{ return(last_date) }}

{% endmacro %}