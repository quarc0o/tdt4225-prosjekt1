"""
WITH mode_count AS (
    SELECT 
        a.user_id, 
        a.transportation_mode,
        COUNT(*) AS mode_count,
        ROW_NUMBER() OVER(
            PARTITION BY a.user_id 
            ORDER BY COUNT(*) DESC, a.transportation_mode ASC
        ) AS row_num
    FROM 
        Activity a
    WHERE 
        a.transportation_mode IS NOT NULL
    GROUP BY 
        a.user_id, a.transportation_mode
)
SELECT 
    mode_count.user_id, 
    mode_count.transportation_mode AS most_used_transportation_mode
FROM 
    mode_count
WHERE 
    mode_count.row_num = 1
ORDER BY 
    mode_count.user_id;
"""