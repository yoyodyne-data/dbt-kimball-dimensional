{%- macro _predicate_lookback_type_partial(data_type, 
                                           lookback_window ) -%} 
    /*{# Enables the lookback window to adjust for data types 

        Args:
            data_type (string) : the standardized data type of the marker column
            lookback_window (int) : the increment for the lookback
    #}*/
    {%- if data_type in ('time','date',) -%}
    {{ xdb.dateadd('day', lookback_window * -1) ,'(SELECT max_value FROM _target_max) ') }}
    {%- else -%}
    ( {{ lookback_window }} * -1) + (SELECT max_value FROM _target_max)
    {%- endif -%}
{%- endmacro -%}


