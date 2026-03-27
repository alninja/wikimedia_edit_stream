{{ config(materialized='view') }}

with raw_data as (
    select *
    from {{ source('staging','raw_stream') }}
    /* Filter out nulls early to keep the data clean */
    where id is not null 
)

select
    -- Identifiers
    cast(id as string) as edit_id,
    cast(wiki as string) as wiki_language,
    
    -- Extracting Language Code (e.g., 'enwiki' -> 'en')
    left(cast(wiki as string), 2) as language_code,

    -- Edit Details
    cast(title as string) as page_title,
    cast(user as string) as user_name,
    cast(bot as boolean) as is_bot,
    
    -- Transforming Boolean to Label for the Dashboard
    CASE 
        WHEN cast(bot as boolean) = true THEN 'Bot'
        WHEN cast(bot as boolean) = false THEN 'Human'
        ELSE 'Unknown'
    END AS user_type,

    cast(type as string) as event_type,
    cast(domain as string) as domain_name,
    
    -- Timestamps
    timestamp_seconds(cast(timestamp as int64)) as event_timestamp,
    
    -- Date for Partitioning
    date(timestamp_seconds(cast(timestamp as int64))) as event_date

from raw_data
