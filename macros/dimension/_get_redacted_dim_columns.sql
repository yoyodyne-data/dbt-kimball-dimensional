{%- macro _get_redacted_dim_columns(existing_relation,type_10_columns) -%}
    /*{# gets the column names and datatypes for the target table minus the added dim columns.
        ARGS:
            - existing_relation (Relation) the DBT relation object.
            - type_10_columns (list) the columns that will have type 10 materializations.
        RETURNS: a list of dicts with column name and data type.
    #}*/
        {% set columns = [] %}
        {% set type_10_column_exclusions = [] %}
        {% for col in type_10_columns %}
            {% do type_10_column_exclusions.append('all_' ~ col | lower ~'_values') %}
        {% endfor %}
        {% set existing_columns = kimball.get_columns_from_existing(existing_relation) %}
        {% for col in existing_columns %}
            {% if col.name |lower not in ['row_effective_at',
                                 'row_expired_at',
                                 'row_is_current',
                                  this.table | lower ~ '_key',
                                  this.table | lower ~ '_id',] + type_10_column_exclusions %}
                {% do columns.append({"name":col.name,
                                      "data_type":col.data_type}) %}
            {% endif %}
        {% endfor %}
        {{ return(columns) }}

{%- endmacro -%}

