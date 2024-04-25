-- add this line for test purpose, to be delete
WITH filter_data_by_date AS (
    SELECT * 
    FROM {{ ref('ga4_source_roll_up') }}
    WHERE epk_division_flag = 'fsh' -- filter on division fashion
    AND geo.country = "France"
    AND event_date BETWEEN '2024-03-01' AND '2024-03-08' --for verify data in recette, to be deleted
),

select_useful_columns AS (
SELECT 
    event_date,
    geo.country,
    user_pseudo_id AS user_id,
    CONCAT(user_pseudo_id, epk_ga_session_id) AS session_id,
    device.category,
    CASE 
        WHEN device.mobile_model_name="iPhone XS Max" then "iPhone 11Pro Max/XS Max"
        WHEN device.mobile_model_name="iPhone+XS+Max" then "iPhone 11Pro Max/XS Max"
        WHEN device.mobile_model_name="iPhone 11Pro Max/XS Max" then "iPhone 11Pro Max/XS Max"
        WHEN device.mobile_model_name="iPhone+11Pro+Max/XS+Max" then "iPhone 11Pro Max/XS Max"
        WHEN device.mobile_model_name="iPhone X/XS" then "iPhone 11Pro/X/XS"
        WHEN device.mobile_model_name="iPhone+X/XS" then "iPhone 11Pro/X/XS"
        WHEN device.mobile_model_name="iPhone 11Pro/X/XS" then "iPhone 11Pro/X/XS" 
        WHEN device.mobile_model_name="iPhone+11Pro/X/XS" then "iPhone 11Pro/X/XS"
        WHEN device.mobile_model_name="iPhone XR" then "iPhone 11/XR"
        WHEN device.mobile_model_name="iPhone+XR" then "iPhone 11/XR"
        WHEN device.mobile_model_name="iPhone 11/XR" then "iPhone 11/XR"
        WHEN device.mobile_model_name="iPhone+11/XR" then "iPhone 11/XR"
        WHEN device.mobile_model_name="iPhone 6+/6S+/7+/8+" then "iPhone 6+/6S+/7+/8+"
        WHEN device.mobile_model_name="iPhone+6+/6S+/7+/8+"then "iPhone 6+/6S+/7+/8+"
        WHEN device.mobile_model_name="iPhone 6/6S/7/8" then "iPhone 6/6S/7/8"
        WHEN device.mobile_model_name="iPhone+6/6S/7/8" then "iPhone 6/6S/7/8"
        WHEN device.mobile_model_name="iPhone 5/5S/5C/SE" then "iPhone 5/5S/5C/SE"
        WHEN device.mobile_model_name="iPhone+5/5S/5C/SE" then "iPhone 5/5S/5C/SE"
        WHEN device.mobile_model_name="iPhone 4/4S" then "iPhone 4/4S"
        WHEN device.mobile_model_name="iPhone+4/4S" then "iPhone 4/4S"
        ELSE device.mobile_model_name
    END AS mobile_model_name,
    device.operating_system,
    device.operating_system_version,
    device.browser,
    device.browser_version
FROM filter_data_by_date
)

SELECT * FROM select_useful_columns