{{ config(
    materialized='table',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["wiki_language", "is_bot"]
) }}

select
    edit_id,
    event_date,
    event_timestamp,
    wiki_language,
    page_title,
    user_name,
    is_bot,
    domain_name,
    event_type
from {{ ref('stg_wikimedia_edits') }}