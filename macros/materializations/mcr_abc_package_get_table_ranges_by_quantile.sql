{%- macro mcr_get_table_ranges_by_quantile(quantile_table_name, quantile_column, count_of_parts=10) -%}

{% set quantiles_table = get_quantiles_table(quantile_table_name, quantile_column, count_of_parts) %}

{% if execute %}
	{%- set query -%}
	WITH

	union_ranges as (

	{% for cur_range in range(count_of_parts) %}
		SELECT
			{{ quantiles_table.rows[cur_range]['range_id'] }}              as range_id, 

			{% if cur_range == 0 -%}
				{{ quantiles_table.rows[cur_range]['min_range'] }}
			{%- else -%}
				{{ quantiles_table.rows[cur_range-1]['quantile_range'] }}
			{%- endif %}                                                   as start_range,

			{% if cur_range+1 == count_of_parts -%}
				{{ quantiles_table.rows[cur_range]['max_range'] }}
			{%- else -%}
				{{ quantiles_table.rows[cur_range]['quantile_range'] }} 
			{%- endif %}                                                   as end_range
			
		{% if not loop.last %}
			UNION ALL
		{% endif %}

	{% endfor %}
	)

	SELECT *
	FROM union_ranges
	ORDER BY range_id

	{% endset %}

{% endif %} {# end if execute #}
    
{%- set query_result = run_query(query) -%}

{{ return(query_result) }}

{%- endmacro -%}


-- MACROS get_quantiles_table() ------------------------------------------------

{% macro get_quantiles_table(quantile_table_name, quantile_column, count_of_parts) %}

{% if execute %}

	{%- set query_text -%}
	WITH 

	quantiles_table as (
		SELECT 
			toString(toFloat64(min(toUInt64({{ quantile_column }}))))  as min_range,
			toString(toFloat64(max(toUInt256({{ quantile_column }})))) as max_range,
			arrayStringConcat(quantiles(
			{% for cur_range in range(count_of_parts) %}
				{{ cur_range+1 }} / {{ count_of_parts }}
				{% if not loop.last %},{% endif %}
			{% endfor %}
			)(toUInt256({{ quantile_column }})), ',')                  as quantiles_acc
		FROM
			{{ ref(quantile_table_name) }} )

	SELECT 
		range_id,
		min_range,
		max_range,
		string_quntile_acc                                             as quantile_range
	FROM 
		quantiles_table
		ARRAY JOIN splitByChar(',', quantiles_acc)                     as string_quntile_acc, 
				arrayEnumerate(splitByChar(',', quantiles_acc))        as range_id
	{%- endset -%}

{% else %} {# if not execute #}

	{%- set query_text -%}
	SELECT 
		0                                                              as range_id,
		'0'                                                            as min_range,
		'0'                                                            as max_range,
		'0'                                                            as quantile_range
	{%- endset -%}

{% endif %} {# end if execute #}

{% set result = run_query(query_text) %}

{{ return(result) }}

{% endmacro %}