SELECT
  a.genre AS genre,
  SUM(t.price) AS total_revenue_by_genre
FROM ticket t
JOIN event e ON t.eventid = e.id
JOIN artist a ON e.artistid = a.id
GROUP BY a.genre
ORDER BY total_revenue_by_genre DESC
LIMIT 10