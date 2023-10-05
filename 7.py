# 7 a)


"""
SELECT COUNT(DISTINCT user_id)
    FROM test_db.Activity
    WHERE DATE(start_date_time) < DATE(end_date_time);
"""


# 7 b)


"""
        SELECT 
            user_id,
            transportation_mode,
            TIMEDIFF(end_date_time, start_date_time) AS duration
        FROM 
            test_db.Activity
        WHERE 
            DATE(start_date_time) < DATE(end_date_time);
"""