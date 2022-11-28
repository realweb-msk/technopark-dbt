{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}


SELECT
    start_date,
    end_date,
    placement,
    CASE 
      WHEN placement = 'Think Mobile' THEN 'In-app'
      WHEN placement = 'Yandex Direct' THEN 'Яндекс.Директ'
      WHEN placement = 'VK Ads' THEN 'VK Реклама'
    ELSE placement END as source,
    platform,
    rate_for_us,
    type,
    adv_type
FROM {{ source('Manual', 'plan_rate') }}
WHERE start_date IS NOT NULL