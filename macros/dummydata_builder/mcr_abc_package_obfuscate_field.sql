{%- macro mcr_abc_package_obfuscate_field(value, type, last_date, data_value_scale_correction=1, counter_for_rand=1) -%}

{%- if type == 'String' -%}
    'dummy_string'
{%- elif type == 'Float64' -%}
    round( {{ value }} * {{ data_value_scale_correction }} )
{%- elif type == 'Date' -%}  
    DATE_ADD(DAY, if(toDate({{ last_date }}) < toStartOfDay( today() ),
      DATE_DIFF(DAY, toDate({{ last_date }}), toStartOfDay( today() )), 0), toDate({{ value }}))
{%- elif type == 'DateTime' -%}
    now()
{%- else -%}
    ''
{%- endif -%}

{%- endmacro -%}