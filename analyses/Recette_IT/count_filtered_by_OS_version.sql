WITH filter_for_recette AS (
    SELECT * 
    FROM {{ ref('IT_WEB') }}
    WHERE country = "France"
    AND event_date BETWEEN '2024-03-01' AND '2024-03-08' 
    AND operating_system_version = 'iOS 17.3.1'
)

SELECT 
    COUNT(DISTINCT user_id) AS distinct_nb_visiters, 
    COUNT(DISTINCT session_id) AS distinct_nb_visits 
FROM 
    filter_for_recette
