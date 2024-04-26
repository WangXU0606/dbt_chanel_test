WITH select_main_columns AS (
    SELECT * 
    FROM {{ ref('main_data_ga4') }}
),

display_all_the_columns AS (
    SELECT 
        select_main_columns.*,
        dim_country.geographical_region,
        event_name,
        ga4_source.epk_collection AS collection,
        ga4_source.epk_division_flag,
        ga4_source.epk_event_action AS event_action,
        ga4_source.epk_event_label,
    FROM select_main_columns
        LEFT JOIN {{ ref('ga4_source_roll_up') }} AS ga4_source
        ON select_main_columns.unique_event_id_modified = ga4_source.unique_event_id_modified
        LEFT JOIN {{ ref('dim_country') }} AS dim_country
        ON select_main_columns.country = dim_country.country
    WHERE ga4_source.epk_event_action LIKE '%video%' -- filter on event video
),

count_video_label AS (
    SELECT 
        display_all_the_columns.*,
        1 AS video,
        CASE WHEN epk_event_label = 'start' THEN 1 END AS start,
        CASE WHEN epk_event_label = '25%' THEN 1 END AS quart,
        CASE WHEN epk_event_label = '50%' THEN 1 END AS monitier,
        CASE WHEN epk_event_label = '75%' THEN 1 END AS troisquart,
        CASE WHEN epk_event_label = '100%' THEN 1 END AS complet
    FROM display_all_the_columns
)

SELECT * FROM count_video_label

