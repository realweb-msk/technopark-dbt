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
    platform,
    rate_for_us,
    type,
    adv_type
FROM `tehnoparkt-bq.Manual.plan_rate`
WHERE start_date IS NOT NULL