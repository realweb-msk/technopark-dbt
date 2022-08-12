{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}


SELECT
    date,
    lower(REPLACE(REPLACE(CampaignName,'+','_'),'-','_')) AS campaign_name,
    'UA' AS campaign_type,
    AdGroupName AS adset_name,
    sum(Impressions) AS impressions,
    sum(Clicks) AS clicks,
    sum(SAFE_CAST(SAFE_DIVIDE(Cost, 1.2) AS FLOAT64)) AS spend
FROM {{ source('MetaCustom', 'yandex_direct_ad_keyword_stat_cpi_technopark_msk') }}
GROUP BY 1,2,3,4