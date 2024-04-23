WITH filter_data_by_date AS (
    SELECT 
        * 
    FROM {{ ref('ga4_source_roll_up') }}
    WHERE epk_division_flag = 'fsh' AND epk_event_action LIKE '%video%' -- filter on division fashion and event video
),

select_useful_columns AS (
    SELECT 
        unique_event_id,
        event_date,
        device.category AS device_category,
        geo.country,
        epk_region AS region,
        epk_ga_session_id,
        event_name,
        epk_collection AS collection,
        epk_division_flag,
        epk_event_action AS event_action,
        epk_event_label,
    FROM filter_data_by_date
),

count_video_label AS (
    SELECT 
        select_useful_columns.*,
        1 AS video,
        CASE WHEN epk_event_label = 'start' THEN 1 END AS start,
        CASE WHEN epk_event_label = '25%' THEN 1 END AS quart,
        CASE WHEN epk_event_label = '50%' THEN 1 END AS monitier,
        CASE WHEN epk_event_label = '75%' THEN 1 END AS troisquart,
        CASE WHEN epk_event_label = '100%' THEN 1 END AS complet
    FROM select_useful_columns
)

SELECT * FROM count_video_label