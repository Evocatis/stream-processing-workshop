SELECT COUNT(DISTINCT str.customerid) AS conversionCount
FROM "stream" str
         JOIN (
    SELECT tix.customerid, evnt.artistid
    FROM ticket tix
             JOIN event evnt ON tix.eventid = evnt.id
) ticketpevent
              ON str.customerid = ticketpevent.customerid
                  AND str.artistid = ticketpevent.artistid;
