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
    adv_type,
    final_cost as plan_budget,
    'UA' as plan_type,
    goal_conversion as plan_order
FROM {{ source('Mediaplan','tehnopark_jule_2022_mp') }}
WHERE start_date IS NOT NULL

UNION ALL

SELECT
    start_date,
    end_date,
    platform,
    placement,
    adv_type,
    final_cost as plan_budget,
    'UA' as plan_type,
    goal_conversion as plan_order
FROM {{ source('Mediaplan','tehnopark_avg_2022_mp') }}
WHERE start_date IS NOT NULL