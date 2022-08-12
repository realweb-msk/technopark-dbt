WITH af_conversions AS (
    SELECT
        date,
        is_retargeting,
        af_cid,
        mediasource,
        platform,
        event_name,
        uniq_event_count,
        event_revenue,
        event_count,
        campaign_name
    FROM  {{ ref('stg_af_data') }}
),


----------------------- yandex -------------------------

yandex_cost AS (
    SELECT
        date,
        campaign_name,
        {{ platform('campaign_name') }} as platform,
        campaign_type,
        sum(impressions) AS impressions,
        sum(clicks) AS clicks,
        sum(spend) AS spend
    FROM {{ ref('int_yandex_cab_meta') }}
    WHERE campaign_type = 'UA'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4
),

yandex_convs_ua AS (
    SELECT 
        date,
        campaign_name,
        platform,
        'UA' as campaign_type,
        sum(IF(event_name = 'install', event_count,0)) AS installs,
        sum(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        sum(IF(event_name = 'first_purchase', event_count, 0)) AS first_purchase,
        sum(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        sum(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        sum(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        sum(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = FALSE
    AND mediasource = 'yandexdirect_int'
    GROUP BY 1,2,3,4
),

yandex_convs_rtg AS (
    SELECT 
        date,
        campaign_name,
        platform,
        'retargeting' AS campaign_type,
        -- информация по покупкам в рет кампаниях должна быть в дашборде UA
        sum(IF(event_name = 'install', 0,0)) AS installs,
        sum(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        sum(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        sum(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        sum(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        sum(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        sum(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = TRUE
    AND mediasource = 'yandexdirect_int'
    GROUP BY 1,2,3,4
),

yandex_convs AS (
    SELECT * FROM yandex_convs_ua
    UNION ALL 
    SELECT * FROM yandex_convs_rtg
),

yandex AS (
    SELECT
        COALESCE(yandex_convs.date, yandex_cost.date) AS date,
        COALESCE(yandex_convs.campaign_name, yandex_cost.campaign_name) AS campaign_name,
        COALESCE(yandex_convs.platform, yandex_cost.platform) AS platform,
        COALESCE(yandex_convs.campaign_type, yandex_cost.campaign_type) AS campaign_type,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'Яндекс.Директ' AS source,
        'context' AS adv_type
    FROM yandex_convs
    FULL OUTER JOIN yandex_cost
    ON yandex_convs.date = yandex_cost.date
    AND yandex_convs.campaign_name = yandex_cost.campaign_name
    WHERE
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(yandex_convs.campaign_name, yandex_cost.campaign_name) != 'None'
),

----------------------- VK Реклама -------------------------

vk_beta_cost AS (
    SELECT
        date,
        campaign_name,
        {{ platform('campaign_name') }} as platform,
        campaign_type,
        sum(impressions) AS impressions,
        sum(clicks) AS clicks,
        sum(cost) AS spend
    FROM {{ ref('stg_vk_beta') }}
    WHERE campaign_type = 'UA'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4
),

vk_beta_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        'UA' as campaign_type,
        sum(IF(event_name = 'install', event_count,0)) AS installs,
        sum(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        sum(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        sum(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        sum(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        sum(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        sum(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = FALSE
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    AND mediasource = 'mail.ru_int'
    GROUP BY 1,2,3,4
),

vk AS (
    SELECT
        COALESCE(vk_beta_convs.date, vk_beta_cost.date) AS date,
        COALESCE(vk_beta_convs.campaign_name, vk_beta_cost.campaign_name) AS campaign_name,
        COALESCE(vk_beta_convs.platform, vk_beta_cost.platform) AS platform,
        COALESCE(vk_beta_convs.campaign_type, vk_beta_cost.campaign_type) AS campaign_type,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'VK Реклама' AS source,
        'social' AS adv_type
    FROM vk_beta_convs
    FULL OUTER JOIN vk_beta_cost
    ON vk_beta_convs.date = vk_beta_cost.date 
    AND vk_beta_convs.campaign_name = vk_beta_cost.campaign_name
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(vk_beta_convs.campaign_name, vk_beta_cost.campaign_name) != 'None'
),

----------------------final----------------------------

unions AS (
    SELECT * FROM yandex
    UNION ALL 
    SELECT * FROM vk
),

final AS (
    SELECT 
        date,
        campaign_name,
        platform,
        campaign_type,
        impressions,
        clicks,
        installs,
        revenue,
        purchase,
        uniq_purchase,
        first_purchase_revenue,
        first_purchase,
        uniq_first_purchase,
        spend,
        source,
        {{ conversion_source_type('campaign_name') }} AS conversion_source_type,
        adv_type
    FROM unions
)

SELECT 
    date,
    campaign_name,
    platform,
    campaign_type,
    impressions,
    clicks,
    installs,
    revenue,
    purchase,
    uniq_purchase,
    first_purchase_revenue,
    first_purchase,
    uniq_first_purchase,
    spend,
    source,
    conversion_source_type,
    adv_type
FROM final