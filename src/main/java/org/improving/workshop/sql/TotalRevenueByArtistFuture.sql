SELECT a.name AS artist_name, SUM(t.price) AS upcoming_revenue
FROM ticket t
         JOIN event e ON t.eventid = e.id
         JOIN artist a ON e.artistid = a.id
WHERE CAST(FROMDATETIME(e.eventdate, 'yyyy-MM-dd HH:mm:ss.SSS') AS BIGINT) > NOW()
GROUP BY a.name
ORDER BY upcoming_revenue DESC
    LIMIT 10