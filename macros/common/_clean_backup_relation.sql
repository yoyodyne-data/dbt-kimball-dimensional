{% macro _clean_backup_relation(target, existing) %}
    /*{# Safely sets and cleans the backup relations. 
    
        Args:
            target (Relation) : the DBT relation target.
            existing (Relation, None) : the existing relation or None if it does not exist.
        
        Returns:
            Relation: the updated backup relation.
    #}*/
    {% set backup_relation = existing.incorporate(
           path={"identifier": target.identifier ~ "__dbt_kimball_backup"} ) %}
    {% if load_relation(backup_relation) is not none %}
        {% do adapter.drop_relation(backup_relation) %}
    {% endif %}
    {% do adapter.rename_relation(target, backup_relation) %}
    {{ return(backup_relation) }}
{% endmacro %}