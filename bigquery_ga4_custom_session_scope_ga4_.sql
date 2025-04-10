-- events, sessions, hostname, etc.
-- created for client xxxxxx from the previous universal analyitcs query
with page_hostname as
(
select
event_date,
event_timestamp,
(select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id,
concat(user_pseudo_id,'-', ( select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
user_pseudo_id,
(select value.string_value from unnest(event_params) where key = 'page_location') as page_location,
device.web_info.hostname,
case
-- direct to checkout may need to stay seperate
    when substr(device.web_info.hostname, 1, 9) = 'checkout.' then substr(device.web_info.hostname, 10)
    when substr(device.web_info.hostname, 1, 5) = 'shop.' then substr(device.web_info.hostname, 6)
    when substr(device.web_info.hostname, 1, 4) = 'www.' then substr(device.web_info.hostname, 5)
    else device.web_info.hostname
  end as extracted_hostname,
  case
    when device.web_info.hostname like '%test%'
    and device.web_info.hostname not like '%deploy%'
    and device.web_info.hostname not like '%sandbox%'
    and device.web_info.hostname not like '%production%'
    and device.web_info.hostname not like '%staging%'
    and device.web_info.hostname not like '%development%'
    and device.web_info.hostname not like '%test%'
    and device.web_info.hostname not like '%local%'
    and device.web_info.hostname not like '%translate%'
    and device.web_info.hostname not like '%capture%'
    and device.web_info.hostname not like '%mall%'
    and device.web_info.hostname not like '%edgemesh%'
    then '1'
    else '0'
end as production_domain_flag,
row_number() over (partition by concat(user_pseudo_id,'-', (select value.int_value from unnest(event_params) where key = 'ga_session_id')) order by event_timestamp asc) as row_num
from
`xxx.analytics_12345678.events_*`
where
_table_suffix between '20240101' and '20240131'
and event_name not in (
'session_start', 'first_visit', 'view_size_guide'
)
),
-- create dataset for landing pages by session
session_landing_pages as (
select
concat(user_pseudo_id,'-', (select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
array_agg
(
  if
	(
      event_name = 'page_view',
      (select value.string_value from unnest(event_params) where key = 'page_title'),
      null
    ) ignore nulls
  order by
  event_timestamp asc
  limit
  1
  ) [safe_offset(0)] as session_landing_page,
array_agg
(
  if
	(
      event_name = 'page_view',
      (select value.string_value from unnest(event_params) where key = 'page_location'),
      null
    ) ignore nulls
  order by
  event_timestamp asc
  limit
  1
  ) [safe_offset(0)] as session_landing_page_link
from 
`xxx.analytics_12345678.events_*`
where
_table_suffix between '20240101' and '20240131'
and event_name = 'page_view'
group by
all
),
-- count page visits per session
page_visits as (
select
session_id,
count(distinct page_location) as pages_per_session,
count(*) as event_count
from
page_hostname
group by
all
),
-- find first and last source
first_last_source as (
select
min(safe_cast(event_date as date format 'YYYYMMDD')) as date,
ga_session_id,
session_id,
user_pseudo_id,
array_agg
(
  if
    (
      coalesce(traffic_source.source,traffic_source.medium,traffic_source.campaign,traffic_source.term,traffic_source.content) is not null,
      traffic_source,
      null
    )
  order by
  event_timestamp asc
  limit
  1
) [safe_offset(0)] as session_first_traffic_source, -- the traffic source of the first event in the session with session_start and first_visit excluded
array_agg
(
  if
    (
      coalesce(traffic_source.source,traffic_source.medium,traffic_source.campaign,traffic_source.term,traffic_source.content) is not null,
      traffic_source,
      null
    ) ignore nulls
  order by
  event_timestamp desc
  limit
  1
-- the last not null traffic source of the session
)[safe_offset(0)] as session_last_traffic_source
from
`xxx.ecomm_analytics.attribution_events`
where
session_id is not null
and event_date between '20240101' and '20240131'
group by
all
),
-- calculate session durations
session_durations as (
select
session_id,
array_agg
(
  event_timestamp
  order by
  event_timestamp asc
  limit
  1
) [safe_offset(0)] as session_first_timestamp,
array_agg
(
  event_timestamp
  order by
  event_timestamp desc
  limit
  1
) [safe_offset(0)] as session_last_timestamp
from
`xxx.ecomm_analytics.attribution_events`
where
session_id is not null
and event_date between '20240101' and '20240131'
group by
all
),
-- pull all purchase events and values
session_purchase_events as 
(
select
concat(user_pseudo_id,'-', (select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id, -- gross revenue - validated
event_value_in_usd,
--( select value.string_value  from unnest(event_params)  where key = 'transaction_id') as order_id 
ecommerce.transaction_id as order_id -- You can use ecommerce.transaction_id instead of unnesting the event_params
from
`xxx.analytics_12345678.events_*`
where
_table_suffix between '20240101' and '20240131'
and event_name = 'purchase'
and event_value_in_usd is not null
and event_value_in_usd > 0
),
-- merge all without grouping
with_last_non_direct_source as (
select
fls.date,
ph.hostname,
ph.extracted_hostname,
ph.production_domain_flag,
case
  when ph.production_domain_flag = '0' 
  then 'US'
  when ph.extracted_hostname = 'test.com' 
  then 'US'
  else 'Intl'
end as region_extracted,
fls.ga_session_id,
fls.session_id,
fls.user_pseudo_id,
fls.session_first_traffic_source,
fls.session_last_traffic_source,
slp.session_landing_page,
slp.session_landing_page_link,
coalesce
  (
    fls.session_first_traffic_source,
    last_value(fls.session_last_traffic_source ignore nulls)
    over (
         partition by fls.user_pseudo_id order by fls.ga_session_id -- 30 day lookback
         range between 2592000 preceding and 1 preceding
         )
  ) as session_first_traffic_source_last_non_direct,
sd.session_last_timestamp - sd.session_first_timestamp as session_duration,
pv.pages_per_session,
case
  when pv.pages_per_session = 1 
  then 1
else 0
end as single_page_rate,
pv.event_count,
case
  when pv.event_count = 1 
  then 1
else 0
end as bounce_rate,
spe.order_id,
spe.event_value_in_usd as revenue
from
first_last_source as fls
left outer join
page_hostname as ph
on fls.session_id = ph.session_id 
and ph.row_num = 1
left outer join
session_landing_pages as slp
on fls.session_id = slp.session_id
left outer join
session_purchase_events as spe
on fls.session_id = spe.session_id
left outer join
session_durations as sd
on fls.session_id = sd.session_id
left outer join
page_visits as pv
on pv.session_id = fls.session_id
)
--group and calculate variables from previous table
select
date,
hostname,
extracted_hostname,
production_domain_flag,
region_extracted,
concat  (coalesce(session_first_traffic_source_last_non_direct.source, '(direct)'),
        ' / ',
        coalesce(session_first_traffic_source_last_non_direct.medium, '(none)'))
        as source_medium,
coalesce(session_first_traffic_source_last_non_direct.source,'(direct)') as source,
coalesce(session_first_traffic_source_last_non_direct.medium,'(none)') as medium,
session_landing_page,
session_landing_page_link,
case
  when production_domain_flag = '0' then 'non production'
  when session_landing_page_link like 'https://test.com/' then 'home'
  when session_landing_page_link like '%collections%' then 'collection'
  when session_landing_page_link like '%gift%' then 'gift guide'
  when session_landing_page_link like '%account%' then 'account'
  when session_landing_page_link like '%pages%' then 'page'
  when session_landing_page_link like '%blogs%' then 'blog'
  when session_landing_page_link like '%products%' then 'product'
  when session_landing_page_link like '%cart%' then 'cart'
  when session_landing_page_link like '%checkout%' then 'checkout'
  when session_landing_page_link like '%thank_you%' then 'purchase'
  when session_landing_page_link not like 'https://test.com/%' then 'intl'
  else 'other'
end as page_type,
round(avg(safe_divide(session_duration,60000000)),2) as avg_session_duration_minutes, -- assuming timestamp is in microseconds and unix time
round(sum(safe_divide(session_duration,60000000)),2) as total_session_duration_minutes,
round(avg(pages_per_session), 2) as pages_per_session, -- distinct pages, does not count revisits
sum(pages_per_session) as total_pages,
format('%.2f', avg(bounce_rate)) as bounce_rate, -- defined as a page visit with no additional events
sum(bounce_rate) as total_bounces,
format('%.2f', avg(single_page_rate)) as single_page_rate, -- defined as a session where only one page is visited (includes scrolls and clicks)
sum(single_page_rate) as total_single_page_sessions,
count(distinct user_pseudo_id) as unique_users,
count(distinct session_id) as sessions,
count(distinct order_id) as orders,
round(count(distinct order_id) / count(distinct session_id),2) as cvr,
format('$%.2f', sum(revenue)) as revenue
from
with_last_non_direct_source
where
hostname is not null  -- hostname and pages_per_session are only null when where clauses in first with statement are not satisfied (excludes sessions where only events are 'session_start', 'first_visit', 'view_size_guide')
and pages_per_session is not null
and session_landing_page is not null -- why are some pages null? (need to investigate)
group by all
order by date desc, sessions desc;

