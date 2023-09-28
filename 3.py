
"""
SELECT 
    Activity.user_id,
    COUNT(Activity.id) AS number_of_activities
FROM 
    test_db.Activity
GROUP BY 
    Activity.user_id
ORDER BY 
    number_of_activities DESC
LIMIT 15;
"""