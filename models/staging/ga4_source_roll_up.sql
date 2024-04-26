with source as (

    select 
        *,
        CONCAT(
            IFNULL(CAST(stream_id AS STRING), '0'), '_',
            IFNULL(CAST(event_date AS STRING), '0'), '_',
            IFNULL(CAST(user_pseudo_id AS STRING), '0'), '_',
            IFNULL(CAST(epk_ga_session_id AS STRING), '0'), '_',
            IFNULL(CAST(event_name AS STRING), '0'), '_',
            IFNULL(CAST(event_timestamp AS STRING), '0'), '_',
            IFNULL(CAST(IFNULL(epk_custom_timestamp, 0) AS STRING), '0'), '_',
            ROW_NUMBER() OVER (
                PARTITION BY 
                    stream_id, 
                    event_date, 
                    user_pseudo_id, 
                    epk_ga_session_id, 
                    event_name, 
                    event_timestamp, 
                    IFNULL(epk_custom_timestamp, 0)
            )
        ) AS unique_event_id_modified -- concatenate differents fields to get the unique identifier        
    from {{ source('ga4_flattened_source', 'flat_events') }}

)

select * from source