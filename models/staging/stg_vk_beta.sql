{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}

WITH source AS (
    SELECT
        date,
        campaign_name,
        sum(COALESCE(spend, 0)) as cost,
        sum(COALESCE(impressions, 0)) as impressions,
        sum(COALESCE(clicks, 0)) as clicks
    FROM {{ source('Manual', 'vk_ads_beta_sheets') }}
    WHERE date IS NOT NULL
    GROUP BY date, campaign_name
),

final AS (
    SELECT
        ARRAY_TO_STRING([
            CAST(date AS STRING),
            campaign_name
        ],'') AS unique_key,
        date,
        campaign_name,
        'UA' AS campaign_type,
        impressions,
        clicks,
        cost
    FROM source
)

SELECT
    unique_key,
    date,
    campaign_name,
    campaign_type,
    impressions,
    clicks,
    cost
FROM final