WITH select_main_columns AS (
    SELECT * 
    FROM {{ ref('main_data_ga4') }}
),

display_all_the_columns AS (
    SELECT 
        select_main_columns.*,
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
        ON select_main_columns.unique_event_id = ga4_source.unique_event_id
),

-- as there are duplicates for the same session, we need to rank them by the event_date and only take the first row
ranked_sessions AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_date) AS row_num
  FROM
    display_all_the_columns
),

first_row_of_each_session AS (
  SELECT
    *
  FROM
    ranked_sessions
  WHERE
    row_num = 1
)

-- 1) As the dashboard IT is at session level, we can drop the column unique_event_id; 2) We calculate the distinct number of sessions and users at this stage to avoid importing too many rows in PowerBI
SELECT
    event_date,
    country,
    locale,
    region,
    category,
    mobile_model_name,
    operating_system,
    operating_system_version,
    browser,
    browser_version, 
    user_id,
    COUNT(DISTINCT session_id) AS total_distinct_sessions
FROM first_row_of_each_session
GROUP BY 
    event_date,
    country,
    locale,
    region,
    category,
    mobile_model_name,
    operating_system,
    operating_system_version,
    browser,
    browser_version,
    user_id
