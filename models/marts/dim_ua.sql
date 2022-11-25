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

yandex_rate AS (
    SELECT
        start_date,
        end_date,
        placement,
        platform,
        rate_for_us,
        type as campaign_type,
        UPPER(adv_type) as adv_type
    FROM {{ ref('stg_rate_info') }}
    WHERE placement = 'Yandex Direct'
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
    WHERE mediasource = 'yandexdirect_int'
    AND is_retargeting = FALSE
    GROUP BY 1,2,3,4
),

yandex_convs_rtg AS (
    SELECT 
        date,
        campaign_name,
        platform,
        'RTG' as campaign_type,
        sum(IF(event_name = 'install', event_count,0)) AS installs,
        sum(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        sum(IF(event_name = 'first_purchase', event_count, 0)) AS first_purchase,
        sum(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        sum(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        sum(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        sum(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE mediasource = 'yandexdirect_int'
    AND is_retargeting = TRUE
    GROUP BY 1,2,3,4
),

yandex_convs AS (
    SELECT * FROM yandex_convs_ua
    UNION ALL
    SELECT * FROM yandex_convs_rtg
),

yandex AS (
    SELECT
        yc.date,
        yc.campaign_name,
        yc.platform,
        yc.campaign_type,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(installs * rate_for_us,0)  AS spend,
        'Yandex Direct' AS source,
        adv_type,
        rate_for_us
    FROM yandex_convs yc
    LEFT JOIN yandex_rate yr
    ON yc.date BETWEEN yr.start_date AND yr.end_date
    AND yc.platform = yr.platform 
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(installs * rate_for_us,0) > 0
    AND campaign_name != 'None'
),

----------------------- VK Реклама -------------------------

 vk_rate AS (
    SELECT
        start_date,
        end_date,
        placement,
        platform,
        rate_for_us,
        type as campaign_type,
        UPPER(adv_type) as adv_type
    FROM {{ ref('stg_rate_info') }}
    WHERE placement = 'VK Ads'
),

vk_convs_ua AS (
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
    WHERE mediasource = 'vk_int'
    AND is_retargeting = FALSE
    GROUP BY 1,2,3,4
),

vk_convs_rtg AS (
    SELECT 
        date,
        campaign_name,
        platform,
        'RTG' as campaign_type,
        sum(IF(event_name = 'install', event_count,0)) AS installs,
        sum(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        sum(IF(event_name = 'first_purchase', event_count, 0)) AS first_purchase,
        sum(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        sum(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        sum(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        sum(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE mediasource = 'vk_int'
    AND is_retargeting = TRUE
    GROUP BY 1,2,3,4
),

vk_convs AS (
    SELECT * FROM vk_convs_ua
    UNION ALL
    SELECT * FROM vk_convs_rtg
),

vk AS (
    SELECT
        vk.date,
        vk.campaign_name,
        vk.platform,
        vk.campaign_type,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(installs * rate_for_us,0)  AS spend,
        'VK Реклама' AS source,
        adv_type,
        rate_for_us
    FROM vk_convs vk
    LEFT JOIN vk_rate vr
    ON vk.date BETWEEN vr.start_date AND vr.end_date
    AND vk.platform = vr.platform 
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(installs * rate_for_us,0) > 0
    AND campaign_name != 'None'
),

----------------------- MyTarget -------------------------

 mt_rate AS (
    SELECT
        start_date,
        end_date,
        placement,
        platform,
        rate_for_us,
        type as campaign_type,
        UPPER(adv_type) as adv_type
    FROM {{ ref('stg_rate_info') }}
    WHERE placement = 'MyTarget'
),

mt_convs_ua AS (
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
    WHERE mediasource = 'mail.ru_int'
    AND is_retargeting = FALSE
    GROUP BY 1,2,3,4
),

mt_convs_rtg AS (
    SELECT 
        date,
        campaign_name,
        platform,
        'RTG' as campaign_type,
        sum(IF(event_name = 'install', event_count,0)) AS installs,
        sum(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        sum(IF(event_name = 'first_purchase', event_count, 0)) AS first_purchase,
        sum(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        sum(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        sum(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        sum(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE mediasource = 'mail.ru_int'
    AND is_retargeting = TRUE
    GROUP BY 1,2,3,4
),

mt_convs AS (
    SELECT * FROM mt_convs_ua
    UNION ALL
    SELECT * FROM mt_convs_rtg
),

mt AS (
    SELECT
        mt.date,
        mt.campaign_name,
        mt.platform,
        mt.campaign_type,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(purchase * rate_for_us,0) AS spend,
        'MyTarget' AS source,
        adv_type,
        rate_for_us
    FROM mt_convs mt
    LEFT JOIN mt_rate mr
    ON mt.date BETWEEN mr.start_date AND mr.end_date
    AND mt.platform = mr.platform 
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(purchase * rate_for_us,0) > 0
    AND campaign_name != 'None'
),

----------------------inapp----------------------------

inapp_rate AS (
    SELECT
        start_date,
        end_date,
        placement,
        platform,
        rate_for_us,
        type as campaign_type,
        UPPER(adv_type) as adv_type
    FROM {{ ref('stg_rate_info') }}
    WHERE placement = 'Think Mobile'
),

inapp_convs_ua AS (
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
    WHERE mediasource = 'mobaigle_int' --тут исправить на корректный
    AND is_retargeting = FALSE
    GROUP BY 1,2,3,4
),

inapp_convs_rtg AS (
    SELECT 
        date,
        campaign_name,
        platform,
        'RTG' as campaign_type,
        sum(IF(event_name = 'install', event_count,0)) AS installs,
        sum(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        sum(IF(event_name = 'first_purchase', event_count, 0)) AS first_purchase,
        sum(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        sum(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        sum(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        sum(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE mediasource = 'mobaigle_int' --тут исправить на корректный
    AND is_retargeting = TRUE
    GROUP BY 1,2,3,4
),

inapp_convs AS (
    SELECT * FROM inapp_convs_ua
    UNION ALL
    SELECT * FROM inapp_convs_rtg
),

inapp AS (
    SELECT
        i.date,
        i.campaign_name,
        i.platform,
        i.campaign_type,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(installs * rate_for_us,0) AS spend,
        placement AS source,
        adv_type,
        rate_for_us
    FROM inapp_convs i
    LEFT JOIN inapp_rate ir
    ON i.date BETWEEN ir.start_date AND ir.end_date
    AND i.platform = ir.platform 
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(installs * rate_for_us,0) > 0
    AND campaign_name != 'None'
),

----------------------final----------------------------

unions AS (
    SELECT * FROM yandex
    UNION ALL 
    SELECT * FROM vk
    UNION ALL 
    SELECT * FROM mt
    UNION ALL 
    SELECT * FROM inapp
),

final AS (
    SELECT 
        date,
        campaign_name,
        platform,
        campaign_type,
        --impressions,
        --clicks,
        installs,
        revenue,
        purchase,
        uniq_purchase,
        first_purchase_revenue,
        first_purchase,
        uniq_first_purchase,
        spend,
        rate_for_us,
        source,
        COALESCE(adv_type, 'Не определено') as adv_type
    FROM unions
)

SELECT * FROM final