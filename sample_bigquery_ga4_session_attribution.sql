-- extract the event data needed for the session traffic source details
WITH events AS (
  SELECT
    CAST(event_date AS date format 'YYYYMMDD') AS date,
    -- unique session id
    CONCAT(user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_start,
    -- wrap all traffic source dimensions into a struct for the next step
    (SELECT
        AS struct 
        collected_traffic_source.manual_source AS source,
        collected_traffic_source.manual_medium AS medium,
        collected_traffic_source.manual_source AS campaign
    ) AS traffic_source,
    event_timestamp
  FROM
    `celestial-sum-327211.analytics_295611372.events_*`

  WHERE
    (_table_suffix >= '20230717' AND _table_suffix <= '20230721')
    AND event_name NOT IN ('session_start', 'first_visit')

), 

events2 as(SELECT
  MIN(date) AS date,
  session_id,
  user_pseudo_id,
  session_start,
  -- the traffic source of the first event in the session with session_start and first_visit excluded
  ARRAY_AGG(
    IF(
      COALESCE(traffic_source.source, traffic_source.medium, traffic_source.campaign) IS NOT NULL,
      traffic_source,
      NULL
    )
    ORDER BY
      event_timestamp asc
    LIMIT 1
  ) [safe_offset(0)] AS session_first_traffic_source,
  -- the last not null traffic source of the session
  ARRAY_AGG(
    IF(
      COALESCE(traffic_source.source,traffic_source.medium,traffic_source.campaign) IS NOT NULL,
      traffic_source,
      null
    ) ignore nulls
    ORDER BY
      event_timestamp DESC
    LIMIT 1
  ) [safe_offset(0)] AS session_last_traffic_source
FROM
  events
WHERE
  session_id IS NOT NULL
GROUP BY
  session_id,
  user_pseudo_id,
  session_start),

  events3 AS (

SELECT
  events.date,
  events.session_id,
  events.user_pseudo_id,
  events.session_start,
  session_first_traffic_source,
  ifnull(
    session_first_traffic_source,
    LAST_VALUE(session_last_traffic_source ignore nulls) OVER(
      PARTITION BY events.user_pseudo_id
      ORDER BY
        events.session_start range between 2592000 preceding
        and current row -- 30 day lookback
    )
  ) as session_traffic_source_last_non_direct

FROM   events left join events2 ON events.session_id = events2.session_id
) 

SELECT
  ifnull(
    session_traffic_source_last_non_direct.source,
    '(direct)'
  ) AS source,
  ifnull(
    session_traffic_source_last_non_direct.medium,
    '(none)'
  ) AS medium,
  count(distinct session_id) AS sessions
FROM
  events3
-- where
--   date between '{start date}'
--   AND '{end date}'
GROUP BY
  1,
  2
ORDER BY
  sessions DESC, source
