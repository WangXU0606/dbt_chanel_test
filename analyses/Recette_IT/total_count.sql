WITH filter_for_recette AS (
    SELECT * 
    FROM `fsh-digital-dashboard-dev`.`wang_dev_DATASET_DASHBOARD`.`IT_WEB`
    WHERE country = "France"
    AND event_date BETWEEN '2024-03-01' AND '2024-03-08' 
)

SELECT 
    COUNT(DISTINCT user_id) AS total_distinct_nb_visiters, 
    COUNT(DISTINCT session_id) AS total_distinct_nb_visits  
FROM 
    filter_for_recette 