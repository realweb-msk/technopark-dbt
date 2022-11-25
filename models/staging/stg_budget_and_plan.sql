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
    adv_type as plan_type,
    plan_cost as plan_budget,
    conversions_plan as plan_order
FROM `tehnoparkt-bq.Manual.plan_rate`
WHERE start_date IS NOT NULL