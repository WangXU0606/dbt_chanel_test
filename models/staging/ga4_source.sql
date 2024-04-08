with source as (

    select * from {{ source('ga4_source_test', 'FSH_DATA') }}

)

select * from source
-- where epk_event_action = 'video_show' -- filter for testing powerBI