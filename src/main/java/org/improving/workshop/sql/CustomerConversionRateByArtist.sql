SELECT
    s.artistid,
    a.name AS artist_name,
    COUNT(DISTINCT t.customerid) AS converted_customers,
    COUNT(DISTINCT s.customerid) AS streamed_customers,
    (COUNT(DISTINCT t.customerid) * 1.0)
        / NULLIF(COUNT(DISTINCT s.customerid), 0) AS conversion_rate

FROM "stream" s
         LEFT JOIN event e
                   ON s.artistid = e.artistid

         LEFT JOIN ticket t
                   ON t.eventid = e.id
                       AND t.customerid = s.customerid

         LEFT JOIN artist a
                   ON s.artistid = a.id

GROUP BY
    s.artistid,
    a.name

ORDER BY conversion_rate DESC;
