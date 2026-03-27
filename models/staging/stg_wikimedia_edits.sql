{{ config(materialized='view') }}

with raw_data as 
(
  select *
  from {{ source('staging','raw_stream') }}
  where id is not null 
)
select
    -- Identifiers
    cast(id as string) as edit_id,
    cast(wiki as string) as wiki_language,
    
    -- Edit Details
    cast(title as string) as page_title,
    cast(user as string) as user_name,
    cast(bot as boolean) as is_bot,
    cast(type as string) as event_type,
    cast(domain as string) as domain_name,
    
    -- Timestamps (Converting the Integer timestamp to a BigQuery Timestamp)
    timestamp_seconds(cast(timestamp as int64)) as event_timestamp,
    
    -- Extract the Date for Partitioning in the next step
    date(timestamp_seconds(cast(timestamp as int64))) as event_date

from raw_data