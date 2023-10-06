

"""
SELECT 
    (SELECT COUNT(*) FROM User) AS UserCount,
    (SELECT COUNT(*) FROM Activity) AS ActivityCount,
    (SELECT COUNT(*) FROM TrackPoint) AS TrackCount;

"""