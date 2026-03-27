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
<<<<<<< HEAD
from {{ ref('stg_wikimedia_edits') }}
=======
from {{ ref('stg_wikimedia_edits') }}
>>>>>>> 4022f4d5ce55e47c6212b0a88e25978ffaff4702
