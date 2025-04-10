SELECT 
  user_pseudo_id,

  ( SELECT value.int_value FROM UNNEST(event_params) WHERE key = "ga_session_id") as ga_session_id,
  -- List of events in order
  STRING_AGG(event_name, '\n' ORDER BY event_timestamp) as concatentated_events,
  -- Event Timestamps
  -- STRING_AGG(FORMAT_TIMESTAMP("%H:%M:%S",TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp),SECOND)), '\n' ORDER BY event_timestamp) as concatentated_event_timestamp,


STRING_AGG(FORMAT_TIMESTAMP("%H:%M:%E3S", TIMESTAMP_MICROS(event_timestamp)), '\n' ORDER BY event_timestamp) AS concatenated_event_timestamp,

  -- Page Location and Page referrer
  STRING_AGG(ifnull( (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),'null'), '\n' order by event_timestamp) as page_location,
  STRING_AGG( ifnull((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer'),'null'), '\n' order by event_timestamp) as page_referrer,


  -- GA4 session source and medium (concatenated) 
  concat(ifnull(traffic_source.source,'null'), ',', ifnull(traffic_source.medium,'null')) as ga4_session_source_medium,
  
  -- GA4 session source and medium (separate) 
  concat(ifnull(traffic_source.source,'null')) as ga4_session_source,
  concat(ifnull(traffic_source.medium,'null')) as ga4_session_medium,

  -- Raw source and medium values for each event (concatenated) 
  STRING_AGG(   concat(   ifnull((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source'),'null'), ',', ifnull((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium'),'null') ), '\n' order by event_timestamp) as event_source_medium,

  -- Raw source and medium values for each event (separate) 
  STRING_AGG(   concat(   ifnull((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source'),'null') ), '\n' order by event_timestamp) as event_params_source,
  STRING_AGG(   concat(   ifnull((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium'),'null') ), '\n' order by event_timestamp) as event_params_medium,
  STRING_AGG(   concat(   ifnull((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign'),'null') ), '\n' order by event_timestamp) as event_params_campaign,



  -- Testing some of the new attribution fields in bigquery
  -- I think collected_traffic_source is supposed to be a copy of the raw data above, but is able to be accessed witout UNNESTing event_params
--   STRING_AGG( concat(ifnull(collected_traffic_source.manual_source,'null'), ',', ifnull(collected_traffic_source.manual_medium,'null')), '\n' order by event_timestamp) as collected_traffic_source_source_medium,
 
--  -- session_traffic_source_last_click.manual_campaign - not sure what this is
  STRING_AGG( ifnull(session_traffic_source_last_click.manual_campaign.source,'null'), '\n' order by event_timestamp) as session_traffic_source_last_click_manual_Source,
  STRING_AGG( ifnull(session_traffic_source_last_click.manual_campaign.medium,'null'), '\n' order by event_timestamp) as session_traffic_source_last_click_manual_Medium,
  STRING_AGG( ifnull(session_traffic_source_last_click.manual_campaign.campaign_name,'null'), '\n' order by event_timestamp) as session_traffic_source_last_click_manual_CampaignName,



-- session_traffic_source_last_click.manual_campaign.campaign_name
  -- Host Name
  STRING_AGG(ifnull( (device.web_info.hostname),'null'), '\n' order by event_timestamp) as host_name,



  count(*)

FROM 
    `project.dataset.events_*`

WHERE 
  _TABLE_SUFFIX BETWEEN '20241210' AND '20250131'
  
-- AND (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'portal') ='xworld'



  group by 1,2,7,8,9


