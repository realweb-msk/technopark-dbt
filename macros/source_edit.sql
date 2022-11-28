{% macro source_edit(campaign_name) %}
CASE 
  WHEN REGEXP_CONTAINS(campaign_name, r'_c210') THEN 'In-app'
  WHEN REGEXP_CONTAINS(campaign_name, r'_c208') THEN 'Яндекс.Директ'
  WHEN REGEXP_CONTAINS(campaign_name, r'_c205') THEN 'MyTarget'
  WHEN REGEXP_CONTAINS(campaign_name, r'_ c207|_c207') THEN 'VK Реклама'
  ELSE mediasource
END
{% endmacro %}