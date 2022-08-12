{% macro conversion_source_type(campaign_name) %}
    CASE 
        WHEN REGEXP_CONTAINS({{campaign_name}}, r'_a999_c205_|_a999_c208_') THEN 'CPI'
    ELSE 'Не определено' END
{% endmacro %}