{%- materialization fact, default -%}
    {% if config.get('lookback_window') is none %}
        {{ kimball._simple_fact() }}
    {% elif config.get('unique_expression') is none %}
        {{ kimball._complex_fact() }}
    {% else %}
        {{ kimball._accumulating_fact() }}
    {% endif %}

{% endmaterialization %}
