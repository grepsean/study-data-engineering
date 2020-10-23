SELECT to_char(ts, 'YYYY-MM') AS month, COUNT(DISTINCT userid) as count_active_user
FROM raw_data.user_session_channel AS A
JOIN raw_data.session_timestamp AS B
ON A.sessionid = B.sessionid
GROUP BY month
ORDER BY month DESC
