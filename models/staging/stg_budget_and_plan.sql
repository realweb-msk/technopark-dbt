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
    platform,
    placement,
    CASE WHEN placement = 'Think Mobile' THEN 'In-app' ELSE placement END as source,
    adv_type as plan_type,
    plan_cost as plan_budget,
    conversions_plan as plan_order
FROM {{ source('Manual', 'plan_rate') }}
WHERE start_date IS NOT NULL