WITH select_useful_columns AS (
    SELECT 
        unique_event_id,
        event_date,
        device.category AS device_category,
        geo.country,
        CASE 
            WHEN geo.country IN ('Austria', 'Belgium', 'Czech Republic', 'Denmark', 'France', 'Germany', 'Italy', 'Luxembourg', 'Netherlands', 'Norway', 'Poland', 'Portugal', 'Russia', 'Spain', 'Sweden', 'Turkey', 'United Arab Emirates') THEN 'Europe'
            WHEN geo.country IN ('Australia', 'Hong Kong S.A.R', 'Korea', 'Mainland China', 'Malaysia', 'Singapore', 'Taiwan Region', 'Thailand', 'Vietnam') THEN 'APAC'
            WHEN geo.country IN ('Brazil', 'Latin America', 'Mexico', 'Switzerland') THEN 'Swiss'
            WHEN geo.country IN ('Canada', 'United Kingdom') THEN 'UK & Others'
            WHEN geo.country = 'United States' THEN 'US'
            WHEN geo.country = 'Japan' THEN 'Japan'
        END AS region,
        epk_ga_session_id,
        event_name,
        epk_collection AS collection,
        epk_division_flag,
        epk_event_action AS event_action,
        epk_event_label,
        CASE 
            WHEN epk_division_flag = 'fsh' AND epk_event_action LIKE '%video%' THEN 'video' 
            ELSE 'other' 
        END AS is_type_video_fashion -- filter to be used in powerBI report
    FROM {{ ref('ga4_source') }}
),
count_video_label AS (
    SELECT 
        select_useful_columns.*,
        COUNT(DISTINCT CASE WHEN is_type_video_fashion = 'video' THEN 1 END) AS video,
        COUNT(DISTINCT CASE WHEN is_type_video_fashion = 'video' AND epk_event_label = '25%' THEN 1 END) AS quart,
        COUNT(DISTINCT CASE WHEN is_type_video_fashion = 'video' AND epk_event_label = '50%' THEN 1 END) AS monitier,
        COUNT(DISTINCT CASE WHEN is_type_video_fashion = 'video' AND epk_event_label = '75%' THEN 1 END) AS troisquart,
        COUNT(DISTINCT CASE WHEN is_type_video_fashion = 'video' AND epk_event_label = '100%' THEN 1 END) AS complet,
        COUNT(DISTINCT CASE WHEN is_type_video_fashion = 'video' AND epk_event_label = 'start' THEN 1 END) AS start
    FROM select_useful_columns
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
)
SELECT * FROM count_video_label