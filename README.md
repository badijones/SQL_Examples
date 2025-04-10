# MySQL_Examples

SQL for a report used to determine how well various hotel chains perform on the booking engine.

Both reports ( mysql_group_report_1.sql and mysql_hotel_payment_report_1..sql ) have similar output.


## Output (header)

| Chain | Started | Abandoned | Completed | %Abandoned | %Completed | %Res | Sent | Pending | MultiRoom | mrcount | Failed | Rescued | Confirmed | Phoned | %Confirmed | Test | Canceled | origRes | NoShow | NotHonored | conf nights |
|-------|---------|-----------|-----------|------------|------------|------|------|---------|-----------|---------|--------|---------|-----------|--------|------------|------|----------|---------|--------|------------|-------------|

===========

# BigQuery GA4 Examples

- The following is an excelent query for answering GA4 attribution questions
   bigquery_ga4_event_scoped_attribution_view.sql

- The following are session scoped ga4 queries I made for various clients
-- bigquery_ga4_custom_session_scope_ga4_.sql
-- bigquery_ga4_session_attribution_modeling.sql
-- bigquery_ga4_session_scoped_ga4_01.sql
