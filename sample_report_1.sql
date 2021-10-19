SELECT IFNULL(hotel_chains.parent,"??") as parent,
    count(*) as Started,
    SUM(IF(status='ABANDONED', 1, 0)) as Abandoned,
    SUM(IF(status !='ABANDONED', 1, 0)) as Compl,
    ROUND((SUM(IF(status='ABANDONED', 1, 0))/(SUM(1) ) )*100,0) as AbandonedRt,
    ROUND((SUM(IF(status !='ABANDONED', 1, 0))/(SUM(1) ) )*100,0) as CompletedRt,
    ROUND(SUM(IF(status='CONFIRMED'|| status='NO SHOW' || status='NOT HONORED' || status='PHONED' || status='CANCELED' || status='TEST', 1, 0))/ (SUM(IF(status='CONFIRMED'|| status='NO SHOW' || status='NOT HONORED' || status='PHONED' || status='CANCELED' || status='TEST' || status='SENT' || status='PENDING' || status='FAILED'  , 1, 0))) *100,0) as ResRt,
    SUM(IF(status='SENT', 1, 0)) as Sent,
    SUM(IF(status='PENDING', 1, 0)) as Pending,
    SUM(IF(status='MULTI-ROOM', 1, 0)) as MultiRoom,
    
SUM(IF(status='MULTI-ROOM', ROUND (   
        (
            LENGTH(reservations.confid)
            - LENGTH( REPLACE ( reservations.confid, ',', '') ) 
        ) / LENGTH(',')        
    ), 0)) as mrcount,

    SUM(IF(status='FAILED', 1, 0)) as Failed,
    SUM(IF(status='RESCUED', 1, 0)) as Rescued,
    SUM(IF(status='CONFIRMED', 1, 0)) as Confirmed,
    SUM(IF( ( promoCode REGEXP('^ ?(GC|MR|HH|PW|CM|MG|MW|CB|CE|TH|SL) ?') AND (status='CONFIRMED') ) , 1, 0)) as 'Phoned',
    ROUND((SUM(IF(status='CONFIRMED'|| (status='PHONED'), 1, 0))/( SUM(IF(status='CONFIRMED'|| status='NO SHOW' || status='NOT HONORED' || status='PHONED' || status='CANCELED' || status='TEST', 1, 0)) ) )*100,0) as ConfRt,
    SUM(IF(status='TEST', 1, 0)) as Test,
    SUM(IF(status='CANCELED', 1, 0)) as Canceled,
    SUM(IF(status='CONFIRMED'|| status='NO SHOW' || status='NOT HONORED' || status='PHONED' || status='CANCELED' || status='TEST', 1, 0)) as 'origRes',
    SUM(IF(status='NO SHOW', 1, 0)) as NoShow,
    SUM(IF(status='NOT HONORED', 1, 0)) as NotHonored,
    SUM(IF(status='CONFIRMED'|| status='SUCCEEDED' || status='PHONED', DATEDIFF( departuredate,arrivaldate), 0)) as confnights,
    ROUND(SUM(IF(status='CONFIRMED'|| status='SUCCEEDED' || status='PHONED', DATEDIFF( departuredate,arrivaldate), 0))/ SUM(IF(status='CONFIRMED', 1, 0)) ,1) as nightsPer
    
     FROM reservations INNER JOIN orders ON orders.orderid = reservations.orderid LEFT JOIN orderaddresses ON orderaddresses.orderidentity = reservations.orderid  
    
    LEFT JOIN hotel_chains ON substr(propertycode,1,2) = hotel_chains.code  

WHERE  orderaddresses.firstname = "Badi" AND orderaddresses.lastname = "Jones"  GROUP BY parent ORDER BY origRes DESC;

 
