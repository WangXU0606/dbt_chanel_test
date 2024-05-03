WITH select_main_columns AS (
    SELECT * 
    FROM {{ ref('main_data_ga4') }}
    WHERE event_date BETWEEN '2024-03-01' AND '2024-03-08'
),

display_all_the_columns AS (
    SELECT 
        select_main_columns.*,
        COALESCE(dim_country.geographical_region, 'other') AS geographical_region, -- This column represents the geographical region for each country and is added based on the mapping condition in the RSE APP dashboard. Contact Yanis SALHENE for questions.
        CASE 
            WHEN ga4_source.device.mobile_model_name="iPhone XS Max" then "iPhone 11Pro Max/XS Max"
            WHEN ga4_source.device.mobile_model_name="iPhone+XS+Max" then "iPhone 11Pro Max/XS Max"
            WHEN ga4_source.device.mobile_model_name="iPhone 11Pro Max/XS Max" then "iPhone 11Pro Max/XS Max"
            WHEN ga4_source.device.mobile_model_name="iPhone+11Pro+Max/XS+Max" then "iPhone 11Pro Max/XS Max"
            WHEN ga4_source.device.mobile_model_name="iPhone X/XS" then "iPhone 11Pro/X/XS"
            WHEN ga4_source.device.mobile_model_name="iPhone+X/XS" then "iPhone 11Pro/X/XS"
            WHEN ga4_source.device.mobile_model_name="iPhone 11Pro/X/XS" then "iPhone 11Pro/X/XS" 
            WHEN ga4_source.device.mobile_model_name="iPhone+11Pro/X/XS" then "iPhone 11Pro/X/XS"
            WHEN ga4_source.device.mobile_model_name="iPhone XR" then "iPhone 11/XR"
            WHEN ga4_source.device.mobile_model_name="iPhone+XR" then "iPhone 11/XR"
            WHEN ga4_source.device.mobile_model_name="iPhone 11/XR" then "iPhone 11/XR"
            WHEN ga4_source.device.mobile_model_name="iPhone+11/XR" then "iPhone 11/XR"
            WHEN ga4_source.device.mobile_model_name="iPhone 6+/6S+/7+/8+" then "iPhone 6+/6S+/7+/8+"
            WHEN ga4_source.device.mobile_model_name="iPhone+6+/6S+/7+/8+"then "iPhone 6+/6S+/7+/8+"
            WHEN ga4_source.device.mobile_model_name="iPhone 6/6S/7/8" then "iPhone 6/6S/7/8"
            WHEN ga4_source.device.mobile_model_name="iPhone+6/6S/7/8" then "iPhone 6/6S/7/8"
            WHEN ga4_source.device.mobile_model_name="iPhone 5/5S/5C/SE" then "iPhone 5/5S/5C/SE"
            WHEN ga4_source.device.mobile_model_name="iPhone+5/5S/5C/SE" then "iPhone 5/5S/5C/SE"
            WHEN ga4_source.device.mobile_model_name="iPhone 4/4S" then "iPhone 4/4S"
            WHEN ga4_source.device.mobile_model_name="iPhone+4/4S" then "iPhone 4/4S"
            ELSE ga4_source.device.mobile_model_name
        END AS mobile_model_name, -- mapping to get the model name, rules are provided by Mathilde FAURE
        ga4_source.device.operating_system,
        ga4_source.device.operating_system_version,
        ga4_source.device.web_info.browser,
        ga4_source.device.web_info.browser_version
    FROM select_main_columns
        LEFT JOIN {{ ref('ga4_source_roll_up') }} AS ga4_source
        ON select_main_columns.unique_event_id_modified = ga4_source.unique_event_id_modified
        LEFT JOIN {{ ref('dim_country') }} AS dim_country
        ON select_main_columns.country = dim_country.country
)

-- SELECT * FROM display_all_the_columns

SELECT DISTINCT  
    event_date,
    user_id,
    session_id,
    country,
    region,
    geographical_region,
    category,
    mobile_model_name,
    operating_system,
    operating_system_version,
    browser,
    browser_version
FROM display_all_the_columns
