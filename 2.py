
# Assume it is talking about number of trackpoints in an activity

"""
SELECT 
    user_id,
    AVG(tp_count.trackpoints_per_activity) AS average_trackpoints,
    MAX(tp_count.trackpoints_per_activity) AS maximum_trackpoints,
    MIN(tp_count.trackpoints_per_activity) AS minimum_trackpoints
FROM 
    (SELECT 
        Activity.id AS activity_id,
        Activity.user_id,
        COUNT(TrackPoint.id) as trackpoints_per_activity
    FROM test_db.TrackPoint
    INNER JOIN test_db.Activity ON TrackPoint.activity_id = Activity.id
    GROUP BY Activity.id, Activity.user_id) AS tp_count
GROUP BY tp_count.user_id;
"""
