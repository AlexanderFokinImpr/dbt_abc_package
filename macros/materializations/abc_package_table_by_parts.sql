{% materialization abc_package_table_by_parts, adapter='clickhouse' %}

-- Info about dbt-core macros here:
-- https://github.com/dbt-labs/dbt-core/tree/main/core/dbt/include/global_project/macros/adapters

-- input data fields
  {%  set input_model              = config.get( 'input_model' ) -%}
  {%  set input_relation           = schema ~'.'~ input_model %}
  {%  set quantile_table           = config.get( 'quantile_table' ) -%}
  {%  set quantile_column          = config.get( 'quantile_column' ) -%}
  {%  set count_of_parts           = config.get( 'count_of_parts', default = 10 ) -%}

-- Define relations

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set backup_relation = none -%}

  {% if existing_relation is not none %}
    {%- set backup_relation_type = existing_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {% endif %}

  --------------------------------------------------------------------------------------------------------------------

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

 --------------------------------------------------------------------------------------------------------------------

  -- Create empty target relation 
  {{ drop_relation_if_exists(target_relation) }}

  {%- set table_ranges_by_quantile = mcr_abc_package_get_table_ranges_by_quantile(quantile_table, quantile_column, count_of_parts) -%}

  {% set target_relation_exists, target_relation = 
                    get_or_create_or_update_relation (  database = none, 
                                                        schema = model.schema, 
                                                        identifier = this.identifier, 
                                                        type = 'table', 
                                                        update = False, 
                                                        temporary = False, 
                                                        sql = select_limit_0(sql), 
                                                        debug_mode = False, 
                                                        silence_mode = False) %}

  -- Run sql queries by queue
  {% for cur_range in table_ranges_by_quantile %}

    {{ log('Iteration ' ~ cur_range['range_id'] ~ ' of 10') }}

    {%- if loop.last -%}
      {%- set last_range = True -%}
    {%- else -%}
      {%- set last_range = False -%}
    {%- endif -%}

    {% set target_part_query = insert_as(sql, target_relation, input_relation, quantile_column, 
                                         cur_range['start_range'], cur_range['end_range']
                                         , last_range) %}

    {% call statement(  'append rows to target') -%}
        {{ target_part_query }}  
    {%- endcall -%}

  {% endfor %}

--------------------------------------------------------------------------------------------------------------------

  {% call noop_statement('main', 'Done') -%} {%- endcall %}

  {{ drop_relation_if_exists(backup_relation) }}

  -----------------------------------------------------

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

-------------------------------------------------------

{% macro get_sql_part_for_insert(sql, input_relation, quantile_column, start_range, end_range, isLast=False) %}
        
  {{ log('start range: ' ~ start_range ~ '| end range: ' ~ end_range) }}

	{% set input_relation_with_where_condition %}
		(   select      *
			from        {{input_relation}} 
			where       toUInt64({{quantile_column}}) >= {{start_range}} 
      {% if not isLast -%}
        and toUInt64({{quantile_column}}) < {{end_range}}
      {%- endif -%}
		)
	{% endset %}

	{% set input_relation_part_insert %}
		WITH            _sql as ({{ sql | replace( input_relation, input_relation_with_where_condition )}})
		SELECT          *
		FROM            _sql
	{% endset %}

  {{ return (input_relation_part_insert) }}

{% endmacro %}

-------------------------------------------------------

{% macro insert_as(sql, target_relation, input_relation, quantile_column, start_range, end_range, isLast=False) %}
        
    {% set inserting_sql = get_sql_part_for_insert(sql, input_relation, quantile_column, start_range, end_range, isLast) %}

    INSERT INTO {{target_relation}} {{inserting_sql}}

    {% set target_relation_part_insert %}
        INSERT INTO {{target_relation}} {{inserting_sql}}
    {% endset %}

    {{ return (target_relation_part_insert)}}
    
{% endmacro %}