SELECT
    COUNT(DISTINCT s.customerid) * 1.0 /
    (SELECT COUNT(DISTINCT customerid) FROM "stream") AS conversionRate
FROM "stream" s
         JOIN (
    SELECT
        t.customerid,
        e.artistid
    FROM ticket t
             JOIN event e
                  ON t.eventid = e.id
) p
              ON s.customerid = p.customerid
                  AND s.artistid = p.artistid;