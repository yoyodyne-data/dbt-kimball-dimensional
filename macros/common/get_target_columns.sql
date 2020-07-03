{%- macro get_columns_from_query(sql) -%}
    /*{# gets the column names and datatypes from the model sql.
        ARGS:
            - sql (string) the base sql statement from the model.
        RETURNS: a list of dicts with column name and data type.
    #}*/
        {% set columns = [] %}
        {% set stub_sql %}
        WITH __dbt_kimball_dimensional_stub AS (
         {{ sql }} 
        )
        SELECT * FROM __dbt_kimball_dimensional_stub LIMIT 1
        {% endset %}
        {% set structure = run_query(stub_sql) %}
        {% for col in structure.columns %}
            {% set dtype = 'time' if 'DateTime' in col.data_type | string 
                           else 'date' if 'Date' in col.data_type | string 
                           else 'number' if 'Number' in col.data_type | string 
                           else 'text' %}
            {% do columns.append({"name":col.name,
                                   "data_type": dtype}) %} 
        {% endfor %}
        {{ return(columns) }}

{%- endmacro -%}

{%- macro get_columns_from_existing(existing_relation) -%}
    /*{# Gets the current column names and datatypes from the existing relation
         and formats them to match the expected lookup object.
         
         Args:
            existing_relation (Relation) : The DBT relation to introspect for columns.
         
         Returns:
            List: a list of column tuples with a format that matches ``get_columns_from_query``.
    #}*/
    {% set columns = [] %}
    {% set existing_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% for col in existing_columns %}
        {% set dtype = 'time' if 'timestamp' in col.dtype | string  
                        else 'date' if 'date' in col.dtype | string 
                        else 'number' if 'integer' in col.dtype | string 
                        else 'number' if 'float' in col.dtype | string 
                        else 'number' if 'numeric' in col.dtype | string 
                        else 'text' %}
            {% do columns.append({"name":col.name,
                                  "data_type":dtype}) %}
        {% endif %}
    {% endfor %}
    {{ return(columns) }}
{%- endmacro -%}
