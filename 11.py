

"""
SELECT a.user_id, COUNT(DISTINCT a.id) AS invalid_count
FROM Activity a
JOIN TrackPoint tp1 ON a.id = tp1.activity_id
JOIN TrackPoint tp2 ON a.id = tp2.activity_id AND tp2.id = tp1.id + 1
WHERE TIMESTAMPDIFF(MINUTE, tp1.date_time, tp2.date_time) >= 5
GROUP BY a.user_id;
"""