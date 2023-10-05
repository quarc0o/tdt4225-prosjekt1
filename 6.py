# No matches

"""
SELECT 
    user_id,
    start_date_time,
    end_date_time,
    transportation_mode,
    COUNT(*) as count
FROM 
    test_db.Activity
GROUP BY 
    user_id, start_date_time, end_date_time, transportation_mode
HAVING 
    COUNT(*) > 1
ORDER BY 
    count DESC;
"""