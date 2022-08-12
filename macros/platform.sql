{% macro platform(campaign_name) %}
    CASE
        WHEN REGEXP_CONTAINS(lower(campaign_name), r'_p02_') THEN 'ios'
        WHEN REGEXP_CONTAINS(lower(campaign_name), r'_p01_') THEN 'android'
    ELSE 'no_platform' END
{% endmacro %}