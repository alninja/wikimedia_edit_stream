{{ config(
    materialized='table',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["wiki_language", "user_type"]
) }}

select
    edit_id,
    wiki_language,         
    language_code,
    page_title,
    user_name,
    user_type,        
    event_type,
    domain_name,
    event_timestamp,
    event_date
from {{ ref('stg_wikimedia_edits') }}
