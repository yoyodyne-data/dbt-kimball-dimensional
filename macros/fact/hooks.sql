{%- macro _kimball_create_meta_fact_table() -%}
    /*{# creates (if not exists) the fact meta table. #}*/
    CREATE TABLE IF NOT EXISTS {{ xdb.fold('dbt_kimball_staging.fact_meta') }} 
    (fact VARCHAR, dimension VARCHAR, instance_at_column VARCHAR, dni_column VARCHAR)
{%- endmacro -%}

{%- macro _kimball_insert_fact_meta(dim, dni_column, instance_at_column) -%}
    /*{# adds meta for the given fact-dim relationship to the staging table.
        ARGS:
            - dim (string) the name of the dimension
            - dni_column (string) optional. The column name to be used for dim lookup as a natural key.
                If none the fact macro will check the model configs, then fall back to the dim DNI.
            - instance_at_column (string) the name of the date or timestamp column to look up the correct SCD row.
                If none the fact macro will check the model configs, then fall back to the dim CDC column.
    #}*/
        {% set dni_column = 'NULL' if dni_column is none else ("'" ~ dni_column ~ "'") %}
        {% set instance_at_column = 'NULL' if instance_at_column is none else ("'" ~ instance_at_column ~ "'") %}
        INSERT INTO {{ xdb.fold('dbt_kimball_staging.fact_meta') }} (fact, dimension, instance_at_column, dni_column)
        VALUES ('{{ this.name }}', '{{ dim }}', {{ instance_at_column }}, {{ dni_column }})
{%- endmacro -%}

