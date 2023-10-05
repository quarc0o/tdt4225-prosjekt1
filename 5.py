"""
SELECT
    Activity.user_id,
    COUNT(DISTINCT Activity.transportation_mode) AS unique_transportation_modes
FROM 
    test_db.Activity
GROUP BY 
    Activity.user_id
ORDER BY 
    unique_transportation_modes DESC
LIMIT 10;
"""