WITH source AS (
    SELECT
        start_date,
        end_date,
        plan_budget,
        platform,
        placement,
        adv_type,
        plan_type,
        plan_order
    FROM {{ ref('stg_budget_and_plan') }}
),

array_table AS (
    SELECT 
        GENERATE_DATE_ARRAY(start_date,end_date) AS date, 
        plan_budget,
        platform,
        placement,
        adv_type, 
        plan_type, 
        plan_order 
    FROM source
),

plans AS (
    SELECT
        date,
        platform,
        placement,
        adv_type,
        plan_budget,
        plan_order,
        CASE
            WHEN plan_type = "UA" THEN "uac"
            ELSE '-' END AS plan_type
    FROM array_table, UNNEST(date) AS date
)

SELECT
    date,
    plan_budget,
    platform,
    placement,
    adv_type,
    plan_order,
    plan_type
FROM plans