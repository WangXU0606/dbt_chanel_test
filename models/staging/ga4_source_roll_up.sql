with source as (

    select 
        *        
    from {{ source('ga4_flattened_source', 'flat_events') }}
    where unique_event_id is not null -- if the unique_event_id is null, then the row does not have sense and should be deleted

)

select * from source