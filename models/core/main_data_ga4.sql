-- This is a core model to filter the 'fsh' division and select the main common columns which will be used later for different dashboards 

WITH filter_data_by_division AS (
    SELECT * 
    FROM {{ ref('ga4_source_roll_up') }}
    WHERE epk_division_flag = 'fsh' -- filter on division fashion
),

select_useful_columns AS (
    SELECT
        unique_event_id,
        event_date,
        user_pseudo_id AS user_id,
        CONCAT(user_pseudo_id, epk_ga_session_id) AS session_id,
        geo.country AS country,
        epk_locale AS locale,
        -- Below countries have multiple regions in the database
        CASE 
            WHEN epk_locale = 'zh_TW' THEN 'apac'
            WHEN epk_locale = 'fr_CH' THEN 'europe' -- quick fix, region to be chosen between europe and swiss
            WHEN epk_locale = 'es_LX' THEN 'swiss' -- quick fix, region to be chosen between uk and swiss
            WHEN epk_locale = 'es_MX' THEN 'swiss' -- quick fix, region to be chosen between uk and swiss
            WHEN epk_locale = 'pt_BR' THEN 'swiss' -- quick fix, region to be chosen between uk and swiss
            WHEN epk_locale = 'it_CH' THEN 'europe' -- quick fix, region to be chosen between europe and swiss
            WHEN epk_locale = 'en_US' THEN 'us'
            WHEN epk_locale = 'ar_AE' THEN 'middle east'
            WHEN epk_locale = 'en_AE' THEN 'middle east'
            WHEN epk_locale = 'de_CH' THEN 'europe' -- quick fix, region to be chosen between europe and swiss
            ELSE epk_region 
        END AS region,
        device.category
    FROM filter_data_by_division
)

SELECT * FROM select_useful_columns
 