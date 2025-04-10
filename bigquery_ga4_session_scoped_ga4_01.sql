-- created for client usng the equivalent universal analytics query

WITH base_events AS (
    SELECT
      (
        SELECT value.int_value 
        FROM UNNEST(event_params) 
        WHERE key = 'ga_session_id'
      ) AS session_id,
      user_pseudo_id,
      device.category AS deviceCategory,
      traffic_source.source AS session_source,
      traffic_source.medium AS session_medium,
      (
        SELECT value.string_value 
        FROM UNNEST(event_params) 
        WHERE key = 'campaign'
      ) AS session_campaign,
      geo.region AS region,
      geo.city AS city,
      geo.country AS country,
      CASE
        WHEN geo.country != 'United States' THEN 'Outside United States'
        ELSE 'United States'
      END AS US_Non_US,
      event_name,
      event_date,
      (
        SELECT value.int_value 
        FROM UNNEST(event_params) 
        WHERE key = 'value'
      ) AS purchase_value
    FROM 
      `xxx-big-query.analytics_1234567.events_*` AS event
    WHERE 
      _TABLE_SUFFIX BETWEEN '20240611' AND '20240811'
  ),
  events AS (
    SELECT
      session_id,
      user_pseudo_id,
      deviceCategory,
      session_source,
      session_medium,
      session_campaign,
      region,
      city,
      event_date,
      event_name,
      US_Non_US,
      COUNTIF(event_name = 'page_view') OVER(PARTITION BY user_pseudo_id, session_id) AS page_view_count,
      SUM(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) OVER(PARTITION BY user_pseudo_id, session_id) AS purchases_qty_session,
      SUM(CASE WHEN event_name = 'purchase' THEN purchase_value ELSE 0 END) OVER(PARTITION BY user_pseudo_id, session_id) AS purchases_total_session
    FROM 
      base_events
  )

SELECT
  PARSE_DATE('%Y%m%d', event_date) AS Date,
  user_pseudo_id,  
  deviceCategory,
  MAX(session_source) AS session_source,
  "" as Keyword,
  MAX(session_medium) AS session_medium,
  
    '' AS AdContent,
  MAX(session_campaign) AS session_campaign,
''as channelGrouping,
  MAX(page_view_count) AS pageviews,

  COUNT(DISTINCT session_id) AS session_count,
  region,
  city,
  '' as country,
    US_Non_US,
purchases_qty_session as Donations,
purchases_total_session as DonationsAmount,
  
FROM 
  events
WHERE 
  session_id IS NOT NULL
 -- AND events.event_name = 'purchase'
GROUP BY 
  user_pseudo_id, deviceCategory, region, city, US_Non_US,Date,purchases_qty_session,purchases_total_session;
