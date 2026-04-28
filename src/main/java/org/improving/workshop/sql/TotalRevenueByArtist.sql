SELECT
    a.id AS artist_id,
    a.name AS artist_name,
    SUM(t.price) AS total_revenue
FROM ticket t
         JOIN event e ON t.eventid = e.id
         JOIN artist a ON e.artistid = a.id
GROUP BY a.id, a.name
ORDER BY total_revenue DESC
    LIMIT 10