WITH select_useful_columns AS (
    SELECT 
        *
    FROM {{ ref('ga4_source') }}
    LIMIT 10
)

SELECT * FROM select_useful_columns