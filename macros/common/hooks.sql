{%- macro _kimball_create_meta_schema() -%}
    /*{# creates (if not exists) the dim meta table. #}*/
    CREATE SCHEMA IF NOT EXISTS {{ xdb.fold('dbt_kimball_staging') }} 
{%- endmacro -%}

{%- macro _kimball_drop_meta_schema() -%}
    /*{# drops (if exists) the meta schema. #}*/
    DROP SCHEMA IF EXISTS {{ xdb.fold('dbt_kimball_staging') }} CASCADE
{%- endmacro -%}