{% macro partner(campaign_name) %}
    CASE 
        WHEN REGEXP_CONTAINS({{campaign_name}}, r'_a799') THEN 'Think Mobile'
    ELSE '-' END
{% endmacro %}