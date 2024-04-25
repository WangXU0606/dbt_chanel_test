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
        CASE 
            WHEN geo.country IN ('Canada', 'United States', 'USA', 'Mexico') THEN 'North America'
            WHEN geo.country IN ('Cyprus', 'Lebanon', 'Syria', 'Iraq', 'Iran', 'Israel', 'Jordan', 'Saudi Arabia', 'Kuwait', 'Qatar', 'Bahrain', 'United Arab Emirates', 'Oman', 'Yemen') THEN 'Middle East'
            WHEN geo.country IN ('Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Guyana', 'French Guiana', 'Paraguay', 'Peru', 'Suriname', 'Uruguay', 'Venezuela') THEN 'Latin America'
            WHEN geo.country IN ('Albania', 'Germany', 'Andorra', 'Austria', 'Belgium', 'Belarus', 'Bosnia and Herzegovina', 'Bulgaria', 'Cyprus', 'Croatia', 'Denmark', 'Spain', 'Estonia', 'Finland', 'France', 'Greece', 'Hungary', 'Ireland', 'Iceland', 'Italy', 'Kosovo', 'Latvia', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Macedonia', 'Painted', 'Moldova', 'Monaco', 'Montenegro', 'Norway', 'Netherlands', 'Poland', 'Portugal', 'Czech Republic', 'Romania', 'United Kingdom', 'Russia', 'San Marino', 'Serbia', 'Slovakia', 'Slovenia', 'Sweden', 'Switzerland', 'Ukraine', 'Vatican') THEN 'Europe'
            WHEN geo.country IN ('China', 'South Korea', 'Hong Kong', 'Indonesia', 'Malaysia', 'Singapore', 'Taiwan', 'Thailand', 'Japan', 'Australia') THEN 'Asia & Pacific'
            WHEN geo.country IN ('Africa', 'South Africa', 'Algeria', 'Angola', 'Benin', 'Botswana', 'Burkina Faso', 'Burundi', 'Cameroon', 'Cape Verde', 'Central African Republic', 'Comoros', 'Republic of the Congo', 'Democratic Republic of the Congo', 'Ivory Coast', 'Djibouti', 'Egypt', 'Eritrea', 'Eswatini', 'Ethiopia', 'Gabon', 'Gambia', 'Ghana', 'Guinea', 'Guinea-Bissau', 'Equatorial Guinea', 'Kenya', 'Lesotho', 'Liberia', 'Libya', 'Madagascar', 'Malawi', 'Mali', 'Morocco', 'Maurice', 'Mauritania', 'Mozambique', 'Namibia', 'Niger', 'Nigeria', 'Uganda', 'Rwanda', 'Sao Tome-et-Principe', 'Senegal', 'Seychelles', 'Sierra Leone', 'Somalia', 'Sudan', 'South Sudan', 'Tanzania', 'Chad', 'Togo', 'Tunisia', 'Zambia', 'Zimbabwe') THEN 'Africa'
            ELSE 'other'
        END AS region_category, -- this mapping is done based on the DAX of RSE APP dashboard
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