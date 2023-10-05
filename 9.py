"""

SELECT 
    user_id, 
    SUM(altitude_diff) AS total_altitude_gain 
FROM (
    SELECT 
        act.user_id,
        tp.altitude - LAG(tp.altitude) OVER(PARTITION BY act.user_id, tp.activity_id ORDER BY tp.date_time) AS altitude_diff
    FROM 
        test_db.TrackPoint tp
    JOIN 
        test_db.Activity act ON tp.activity_id = act.id
    WHERE 
        tp.altitude IS NOT NULL AND 
        tp.altitude >= 0  -- assuming altitude cannot be negative and invalid values are either NULL or negative
) AS altitude_differences
WHERE 
    altitude_diff > 0 -- only consider increases in altitude
GROUP BY 
    user_id
ORDER BY 
    total_altitude_gain DESC
LIMIT 15;

"""