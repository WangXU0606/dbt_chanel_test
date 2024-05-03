WITH filter_for_recette AS (
    SELECT * 
    FROM {{ ref('RSE_WEB') }}
    WHERE event_date BETWEEN '2023-09-11' AND '2023-09-18' 
    AND event_action IN ("video_campaign", "video_show", "video_vip")
)

SELECT 
    event_action, 
    COUNT(*) 
FROM 
    filter_for_recette
GROUP BY 
    event_action