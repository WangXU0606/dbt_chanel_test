WITH filter_for_recette AS (
    SELECT * 
    FROM {{ ref('RSE_WEB') }}
    WHERE event_date BETWEEN '2023-09-11' AND '2023-09-18' 
    AND event_action = "video_campaign"
)

SELECT 
    SUM(start) AS count_start, 
    SUM(quart) AS count_quart, 
    SUM(monitier) AS count_monitier, 
    SUM(troisquart) AS count_troisquart, 
    SUM(complet) AS count_complet 
FROM 
    filter_for_recette