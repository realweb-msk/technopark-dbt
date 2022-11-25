WITH source AS (
    SELECT
        start_date,
        end_date,
        platform,
        placement,
        plan_type,
        plan_budget,
        plan_order
    FROM {{ ref('stg_budget_and_plan') }}
),

array_table AS (
    SELECT 
        GENERATE_DATE_ARRAY(start_date,end_date) AS date, 
        platform,
        placement,
        plan_type,
        plan_budget,
        plan_order 
    FROM source
),

plans AS (
    SELECT
        date,
        platform,
        placement,
        plan_type,
        plan_budget,
        plan_order
    FROM array_table, UNNEST(date) AS date
)

SELECT
    date,
    platform,
    placement,
    plan_type,
    plan_budget,
    plan_order
FROM plans