WITH filter_for_recette AS (
    SELECT * 
    FROM `fsh-digital-dashboard-dev`.`wang_dev_DATASET_DASHBOARD`.`IT_WEB`
    WHERE country = "France"
    AND event_date BETWEEN '2024-03-01' AND '2024-03-08' 
)

SELECT 
    operating_system, 
    COUNT(DISTINCT user_id) AS distinct_nb_visiters, 
    COUNT(DISTINCT session_id) AS distinct_nb_visits  
FROM 
    filter_for_recette
GROUP BY 
    operating_system
