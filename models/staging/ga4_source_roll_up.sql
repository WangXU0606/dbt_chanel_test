with source as (

    select * from {{ source('ga4_flattened_source', 'flat_events') }}

)

select * from source