SELECT 
IFNULL(hotel_chains.parent,"NA") as 'Family Name',	


    SUM(IF(reservations.status='CONFIRMED'|| reservations.status='NO SHOW' || reservations.status='NOT HONORED' || reservations.status='PHONED' || reservations.status='CANCELED', 1, 0)) as 'OrigRes',
    SUM(IF(collections.status IN('P','PM','PL','PR'), 1,0)) as 'PMLR',
    SUM(IF(collections.status ='C' || (reservations.status IN( 'CANCELED') && collections.status IS NULL), 1, 0)) as C,
    SUM(IF(collections.status ='NS'     || (reservations.status = 'NO SHOW' && collections.status IS NULL), 1, 0)) as 'NS',
    SUM(IF(collections.status ='NC', 1, 0)) as NC,
    SUM(IF(collections.status ='UC' || (reservations.status = 'NOT HONORED' && collections.status IS NULL) , 1, 0)) as UC,
    SUM(IF(collections.status ='CS', 1, 0)) as CS,
    SUM(IF(collections.status ='DP', 1, 0)) as DP,
    SUM(IF(collections.status ='NF', 1, 0)) as NF,
    SUM(IF(collections.status ='NR', 1, 0)) as NR,
    SUM(IF(collections.status ='Z', 1, 0)) as Z,
    SUM(IF(collections.status ='6', 1, 0)) as '6',
    SUM(IF(collections.status NOT IN('P','PM','PL','PR','C','NS','NC','UC','CS','DP','NF','NR','Z','6') AND collections.status IS NOT NULL, 1, 0)) as Other,
  
  
  
  
  
    SUM(IF(collections.status IS NOT NULL, 1, 0)) as 'Non Pending',
    ( SUM(IF(  reservations.status='CONFIRMED'|| reservations.status='NOT HONORED' || reservations.status='PHONED' || ((reservations.status = 'CANCELED' || reservations.status = 'NO SHOW') && collections.status IS NOT NULL) , 1, 0)) - SUM(IF(collections.status IS NOT NULL, 1, 0)) ) as 'Pending',
    
     ROUND((   ( SUM(IF(   reservations.status='CONFIRMED'|| reservations.status='NO SHOW' || reservations.status='NOT HONORED' || reservations.status='PHONED' || reservations.status='CANCELED', 1, 0)) - SUM(IF(collections.status IS NOT NULL, 1, 0)) )  /  SUM(IF(   reservations.status='CONFIRMED'|| reservations.status='NO SHOW' || reservations.status='NOT HONORED' || reservations.status='PHONED' || reservations.status='CANCELED', 1, 0))  )*100)     as '% Pending',
     	
     ROUND((   SUM(IF( collections.status IN('P','PM','PL','PR'), 1,0))  /  SUM(IF(   reservations.status='CONFIRMED'|| reservations.status='NO SHOW' || reservations.status='NOT HONORED' || reservations.status='PHONED' || reservations.status='CANCELED', 1, 0))  )*100)     as '% Paid'
     
     

    

FROM reservations 
LEFT JOIN collections on collections.confid = reservations.confid AND collections.status != 'NT0' AND collections.confid != '' AND reservations.confid!=''

LEFT JOIN hotel_chains ON substr(propertycode,1,2) = hotel_chains.code  


WHERE  reservations.prop_id != ''

AND ( reservations.status='CONFIRMED'|| reservations.status='NO SHOW' || reservations.status='NOT HONORED' || reservations.status='PHONED' || reservations.status='CANCELED')
AND reservations.compid !=0


AND (reservations.departuredate >= '2021-09-01 00:00:00' AND reservations.departuredate <= '2021-10-19 23:59:59')


GROUP BY IFNULL(hotel_chains.parent,"NA") ;
