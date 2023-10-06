
# Assume it is talking about number of trackpoints in an activity

"""
SELECT 
    user_id,
    AVG(tp_count.trackpoints_per_activity) AS avg_tp,
    MAX(tp_count.trackpoints_per_activity) AS max_tp,
    MIN(tp_count.trackpoints_per_activity) AS min_tp
FROM 
    (SELECT 
        Activity.id AS activity_id,
        Activity.user_id,
        COUNT(TrackPoint.id) as trackpoints_per_activity
    FROM TrackPoint
    INNER JOIN Activity ON TrackPoint.activity_id = Activity.id
    GROUP BY Activity.id, Activity.user_id) AS tp_count
GROUP BY tp_count.user_id;
"""
