-- replicate GA4 session scoped attribution
WITH base_events AS (
  SELECT
    CAST(event_date AS DATE FORMAT 'YYYYMMDD') AS date,
    -- unique session id
    CONCAT(user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_start,
    -- wrap all traffic source dimensions into a struct for the next step
    (SELECT AS STRUCT 
        collected_traffic_source.manual_source AS source,
        collected_traffic_source.manual_medium AS medium,
        collected_traffic_source.manual_source AS campaign
    ) AS traffic_source,
    event_timestamp
  FROM
    `celestial-sum-327211.analytics_295611372.events_*`

  WHERE
    _TABLE_SUFFIX BETWEEN '20230717' AND '20230721'
    AND event_name NOT IN ('session_start', 'first_visit')
), 

session_traffic_sources AS (
  SELECT
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
      ORDER BY event_timestamp ASC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS session_first_traffic_source,
  -- the last not null traffic source of the session
    ARRAY_AGG(
      IF(
        COALESCE(traffic_source.source, traffic_source.medium, traffic_source.campaign) IS NOT NULL,
        traffic_source,
        NULL
      )
      IGNORE NULLS
      ORDER BY event_timestamp DESC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS session_last_traffic_source
  FROM base_events
  WHERE session_id IS NOT NULL
  GROUP BY session_id, user_pseudo_id, session_start
), 

attributed_sessions AS (
  SELECT
    base_events.date,
    base_events.session_id,
    base_events.user_pseudo_id,
    base_events.session_start,
    session_first_traffic_source,
    IFNULL(
      session_first_traffic_source,
      LAST_VALUE(session_last_traffic_source IGNORE NULLS) OVER (
        PARTITION BY base_events.user_pseudo_id
        ORDER BY base_events.session_start
        RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW -- 30 day lookback
      )
    ) AS session_traffic_source_last_non_direct
  FROM base_events
  LEFT JOIN session_traffic_sources USING (session_id)
)

SELECT
  IFNULL(session_traffic_source_last_non_direct.source, '(direct)') AS source,
  IFNULL(session_traffic_source_last_non_direct.medium, '(none)') AS medium,
  COUNT(DISTINCT session_id) AS sessions
FROM attributed_sessions
GROUP BY 1, 2
ORDER BY sessions DESC, source;
