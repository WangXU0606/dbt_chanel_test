-- This is a core model to filter the 'fsh' division and select the main common columns which will be used later for different dashboards 

WITH filter_data_by_division AS (
    SELECT * 
    FROM {{ ref('ga4_source_roll_up') }}
    WHERE epk_division_flag = 'fsh' -- filter on division fashion
),

select_useful_columns AS (
    SELECT
        unique_event_id_modified,
        event_date,
        user_pseudo_id AS user_id,
        CONCAT(user_pseudo_id, epk_ga_session_id) AS session_id,
        geo.country AS country,
        epk_region AS region,
        device.category
    FROM filter_data_by_division
)

SELECT * FROM select_useful_columns
 