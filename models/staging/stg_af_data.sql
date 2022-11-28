{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}

WITH 
appsflyaer_raw_data AS (
  SELECT DISTINCT
    date(event_time) as event_time,
    is_retargeting,
    campaign as campaign_name,
    campaign_id as af_cid,
    adset as af_adset,
    media_source as Media_Source,
    platform,
    event_value,
    event_name as Event_Name,
    appsflyer_id,
    event_revenue as Event_Revenue,
    date(install_time) as install_time,
  FROM {{ ref('stg_appsflyer_inapp_events') }}
),
--костыль: для одного appsflyer_id первая покупка должна быть определена только один раз--
appsflyaer_raw_data_fp AS (
  SELECT
    event_time,
    is_retargeting,
    campaign_name,
    af_cid,
    af_adset,
    Media_Source,
    platform,
    event_value,
    Event_Name,
    appsflyer_id,
    Event_Revenue,
    install_time,
    ROW_NUMBER() OVER(PARTITION BY appsflyer_id, event_name, is_retargeting ORDER BY event_time) rn
  FROM appsflyaer_raw_data
),

af_data_first_purchase AS (
  SELECT
    event_time AS date,
    is_retargeting,
    campaign_name,
    af_cid,
    af_adset,
    COALESCE(Media_Source,'no') as mediasource,
    platform,
    event_value,
    Event_Name as event_name,
    count(distinct appsflyer_id) as uniq_event_count,
    sum(CAST(Event_Revenue AS NUMERIC)) as event_revenue,
    count(Event_Name) AS event_count,
 
  FROM appsflyaer_raw_data_fp
  WHERE Event_Name = 'first_purchase'
        AND rn=1
        --условие отбора - событие должно произойти в том же месяце, в котором была установка
        AND DATE_TRUNC(install_time, MONTH) = DATE_TRUNC(event_time, MONTH)
        AND COALESCE(Media_Source,'no') not in ('organic')
  GROUP BY 1,2,3,4,5,6,7,8,9

  UNION ALL 

  SELECT
    IF(
       (EXTRACT(DAY FROM event_time)<=3 
        AND DATE_TRUNC(install_time, MONTH) = DATE_ADD(DATE_TRUNC(event_time, MONTH), INTERVAL -1 MONTH)),

        DATE_ADD(event_time, INTERVAL -1 MONTH), event_time
        ) as date,
    is_retargeting,
    campaign_name,
    af_cid,
    af_adset,
    COALESCE(Media_Source,'no') as mediasource,
    platform,
    event_value,
    Event_Name as event_name,
    count(distinct appsflyer_id) as uniq_event_count,
    sum(CAST(Event_Revenue AS NUMERIC)) as event_revenue,
    count(Event_Name) AS event_count
  FROM appsflyaer_raw_data_fp
  WHERE Event_Name = 'first_purchase'
        AND rn=1
        
        AND ((EXTRACT(DAY FROM event_time)<=3 
            AND DATE_TRUNC(install_time, MONTH) = DATE_ADD(DATE_TRUNC(event_time, MONTH), INTERVAL -1 MONTH))
            
            OR (DATE_TRUNC(install_time, MONTH) = DATE_TRUNC(event_time, MONTH)))
        AND COALESCE(Media_Source,'no') not in ('organic')
  GROUP BY 1,2,3,4,5,6,7,8,9
  ORDER BY 1
),

af_data_event AS (
  SELECT
    date(event_time) AS date,
    is_retargeting,
    campaign_name,
    af_cid,
    af_adset,
    COALESCE(Media_Source,'no') as mediasource,
    platform,
    event_value,
    Event_Name as event_name,
    count(distinct appsflyer_id) as uniq_event_count,
    sum(CAST(Event_Revenue AS NUMERIC)) as event_revenue,
    count(Event_Name) AS event_count
  FROM appsflyaer_raw_data
  WHERE Event_Name IN ('af_purchase','af_complete_registration') 
        --условие отбора - событие должно произойти в том же месяце, в котором была установка
        AND DATE_TRUNC(install_time, MONTH) = DATE_TRUNC(event_time, MONTH)
            
        AND COALESCE(Media_Source,'no') not in ('organic')
  GROUP BY 1,2,3,4,5,6,7,8,9

  UNION ALL 

  SELECT
    IF(
       (EXTRACT(DAY FROM event_time)<=3 
        AND DATE_TRUNC(install_time, MONTH) = DATE_ADD(DATE_TRUNC(event_time, MONTH), INTERVAL -1 MONTH)),

        DATE_ADD(event_time, INTERVAL -1 MONTH), event_time
        ) as date,
    is_retargeting,
    campaign_name,
    af_cid,
    af_adset,
    COALESCE(Media_Source,'no') as mediasource,
    platform,
    event_value,
    Event_Name as event_name,
    count(distinct appsflyer_id) as uniq_event_count,
    sum(CAST(Event_Revenue AS NUMERIC)) as event_revenue,
    count(Event_Name) AS event_count
  FROM appsflyaer_raw_data
  WHERE Event_Name IN ('af_purchase','af_complete_registration') 
        -- в отчет включаются пользователи, сделавшие событие в первые три дня месяца, но с установкой в прошлом месяце
        -- + 
        --пользователи, сделавшие событие в том же месяце что и установку
        AND ((EXTRACT(DAY FROM event_time)<=3 
            AND DATE_TRUNC(install_time, MONTH) = DATE_ADD(DATE_TRUNC(event_time, MONTH), INTERVAL -1 MONTH))
            
            OR (DATE_TRUNC(install_time, MONTH) = DATE_TRUNC(event_time, MONTH)))
        AND COALESCE(Media_Source,'no') not in ('organic')
        
  GROUP BY 1,2,3,4,5,6,7,8,9
  ORDER BY 1
),
af_data_install AS (
  SELECT
    date(event_time) AS date,
    is_retargeting,
    campaign as campaign_name,
    campaign_id as af_cid,
    adset as af_adset,
    COALESCE(media_source,'no') as mediasource,
    platform,
    event_value,
    event_name as event_name,
    count(distinct appsflyer_id) as uniq_event_count,
    0 as event_revenue,
    count(event_name) AS event_count
    FROM {{ ref('stg_appsflyer_installs') }}
    WHERE event_name in ('install','re-attribution','re-engagement') AND 
          COALESCE(Media_Source,'no') not in ('organic')

  GROUP BY 1,2,3,4,5,6,7,8,9
)

SELECT 
  date,
  is_retargeting,
  campaign_name,
  af_cid,
  af_adset,
  mediasource,
  {{ source_edit('campaign_name') }} as source,
  platform,
  event_value,
  event_name,
  uniq_event_count,
  event_revenue,
  event_count
FROM (
  SELECT * FROM af_data_event
  UNION ALL 
  SELECT * FROM af_data_install
  UNION ALL
  SELECT * FROM af_data_first_purchase
  ORDER BY 1) AS T
