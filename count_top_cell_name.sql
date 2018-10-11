select
SUSG_REF_NUM,
CELL_NAME,
cast(b.lat3_avg as real) as latitude,
--cast(LEAD(b.lat3_avg,1,0) OVER (PARTITION BY b.SUSG_REF_NUM ORDER BY b.RECORDOPENINGTIME) as real) as latitude_next,
cast(b.lon3_avg as real) as longitude,
--cast(LEAD(b.lon3_avg,1,0) OVER (PARTITION BY b.SUSG_REF_NUM ORDER BY b.RECORDOPENINGTIME) as real) as longitude_next,
count(1) as c

from (
select
SUM(lat0+lat1+lat2)/3 as lat3_avg,
SUM(lon0+lon1+lon2)/3 as lon3_avg,
SUBSTR(CELL_NAME, 1, 5) as CELL_NAME,
SUSG_REF_NUM,
SERVEDMSISDN,
RECORDOPENINGTIME
from (
SELECT
S.SUSG_REF_NUM,
S.RECORDOPENINGTIME,
S.SERVEDMSISDN,
N.CELL_NAME,
N.LATITUDE as lat0,
N.LONGITUDE as lon0,
LEAD(N.LATITUDE,1,0) OVER 
(PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lat1,
LEAD(N.LONGITUDE,1,0) OVER 
(PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lon1,
LEAD(N.LATITUDE,2,0) OVER 
(PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lat2,
LEAD(N.LONGITUDE,2,0) OVER 
(PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lon2,
S.RACT_TYPE,
S.DATAVOLUMNEGPRSUPLINK,
S.DATAVOLUMNEGPRSDOWNLINK,
N.AREA_CODE
FROM    DWH_PROD.SGSN_FACTS S
LEFT OUTER JOIN  DWH_PROD.NETWORK_NODE N ON (S.NETWORK_NODE_ID = N.ID) 
WHERE S.RECORDOPENINGTIME >= '2018-02-01'
AND S.RECORDOPENINGTIME < '2018-03-01'
AND SERVEDMSISDN = '53453342'
) a
group by 3,4,5,6
order by 6
) b
group by 1,2,3,4;


                                    
select min(RECORDOPENINGTIME) from DWH_PROD.SGSN_FACTS;

create table alexsi.test as 
select 
cast(b.LATITUDE as real) as latitude_real,
cast(b.LONGITUDE as real) as longitude_real,
LATITUDE,
LONGITUDE
from (
                                SELECT S.SUSG_REF_NUM,
                                    S.RECORDOPENINGTIME,
                                    S.SERVEDMSISDN,
                                    N.CELL_NAME,
                                    N.LATITUDE ,
                                    N.LONGITUDE,
                                    S.DURATION
                                
                                    FROM    DWH_PROD.SGSN_FACTS S
                                    LEFT OUTER JOIN  DWH_PROD.NETWORK_NODE N ON (S.NETWORK_NODE_ID = N.ID) 
                                    WHERE S.RECORDOPENINGTIME >= '2018-04-05'
                                    AND S.RECORDOPENINGTIME < '2018-04-06'
                                    AND SERVEDMSISDN = '53453342'
                                    --AND SERVEDMSISDN = '53086721'
                                    
                                    ) b
                                     ;
select   
distinct
CELL_NAME,
LATITUDE,
LONGITUDE,
SUSG_REF_NUM,
SERVEDMSISDN
from (
        select
        SUM(lat0+lat1+lat2)/3 as LATITUDE,
        SUM(lon0+lon1+lon2)/3 as LONGITUDE,
        SUBSTR(CELL_NAME, 1, 5) as CELL_NAME,
        SUSG_REF_NUM,
        SERVEDMSISDN,
        RECORDOPENINGTIME
        from (
                SELECT
                S.SUSG_REF_NUM,
                S.RECORDOPENINGTIME,
                S.SERVEDMSISDN,
                N.CELL_NAME,
                N.LATITUDE as lat0,
                N.LONGITUDE as lon0,
                LEAD(N.LATITUDE,1,0) OVER (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lat1,
                LEAD(N.LONGITUDE,1,0) OVER (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lon1,
                LEAD(N.LATITUDE,2,0) OVER  (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lat2,
                LEAD(N.LONGITUDE,2,0) OVER (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lon2,
                S.RACT_TYPE,
                S.DATAVOLUMNEGPRSUPLINK,
                S.DATAVOLUMNEGPRSDOWNLINK,
                N.AREA_CODE
                FROM    DWH_PROD.SGSN_FACTS S
                LEFT OUTER JOIN  DWH_PROD.NETWORK_NODE N ON (S.NETWORK_NODE_ID = N.ID) 
                WHERE S.RECORDOPENINGTIME >= '2018-04-05'
                AND S.RECORDOPENINGTIME < '2018-04-06'
                AND SERVEDMSISDN = '53453342'
                ) a
        group by 3,4,5,6
        order by 6     
        ) b
where      
LATITUDE < 100 
and LONGITUDE < 30;



-- moving avarage
---
SELECT ts, bid, AVG(bid)
   OVER(ORDER BY ts
       RANGE BETWEEN INTERVAL '40 seconds' 
       PRECEDING AND CURRENT ROW)
FROM ticks 
WHERE stock = 'abc' 
GROUP BY bid, ts 
ORDER BY ts;
---


select distinct
SERVEDMSISDN,
SUSG_REF_NUM,   
CELL_NAME,
AVG(LATITUDE) OVER(ORDER BY RECORDOPENINGTIME
       RANGE BETWEEN INTERVAL '10 minutes' 
       PRECEDING AND CURRENT ROW) as LATITUDE,
AVG(LONGITUDE) OVER(ORDER BY RECORDOPENINGTIME
       RANGE BETWEEN INTERVAL '10 minutes' 
       PRECEDING AND CURRENT ROW) as LONGITUDE
from (
        select
        SUM(lat0+lat1+lat2)/3 as LATITUDE,
        SUM(lon0+lon1+lon2)/3 as LONGITUDE,
        SUBSTR(CELL_NAME, 1, 5) as CELL_NAME,
        SUSG_REF_NUM,
        SERVEDMSISDN,
        RECORDOPENINGTIME
        from (
                SELECT
                S.SUSG_REF_NUM,
                S.RECORDOPENINGTIME,
                S.SERVEDMSISDN,
                N.CELL_NAME,
                N.LATITUDE as lat0,
                N.LONGITUDE as lon0,
                LEAD(N.LATITUDE,1,0) OVER (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lat1,
                LEAD(N.LONGITUDE,1,0) OVER (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lon1,
                LEAD(N.LATITUDE,2,0) OVER  (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lat2,
                LEAD(N.LONGITUDE,2,0) OVER (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lon2,
                S.RACT_TYPE,
                S.DATAVOLUMNEGPRSUPLINK,
                S.DATAVOLUMNEGPRSDOWNLINK,
                N.AREA_CODE
                FROM    DWH_PROD.SGSN_FACTS S
                LEFT OUTER JOIN  DWH_PROD.NETWORK_NODE N ON (S.NETWORK_NODE_ID = N.ID) 
                WHERE S.RECORDOPENINGTIME >= '2018-04-05'
                AND S.RECORDOPENINGTIME < '2018-04-06'
                AND SERVEDMSISDN = '53453342'
                ) a
        group by 3,4,5,6
        order by 6     
        ) b
where      
LATITUDE < 100 
and LONGITUDE < 30
group by 
SERVEDMSISDN, 
SUSG_REF_NUM,  
CELL_NAME,
LATITUDE,
LONGITUDE,
RECORDOPENINGTIME
order by 1,2,3
;


---------
--avarage
---------

select distinct
SERVEDMSISDN,
SUSG_REF_NUM,   
CELL_NAME,
AVG(LATITUDE)  as LATITUDE,
AVG(LONGITUDE) as LONGITUDE
from (
        select
        SUM(lat0+lat1+lat2)/3 as LATITUDE,
        SUM(lon0+lon1+lon2)/3 as LONGITUDE,
        SUBSTR(CELL_NAME, 1, 5) as CELL_NAME,
        SUSG_REF_NUM,
        SERVEDMSISDN,
        RECORDOPENINGTIME
        from (
                SELECT
                S.SUSG_REF_NUM,
                S.RECORDOPENINGTIME,
                S.SERVEDMSISDN,
                N.CELL_NAME,
                N.LATITUDE as lat0,
                N.LONGITUDE as lon0,
                LEAD(N.LATITUDE,1,0) OVER (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lat1,
                LEAD(N.LONGITUDE,1,0) OVER (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lon1,
                LEAD(N.LATITUDE,2,0) OVER  (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lat2,
                LEAD(N.LONGITUDE,2,0) OVER (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lon2

                FROM    DWH_PROD.SGSN_FACTS S
                LEFT OUTER JOIN  DWH_PROD.NETWORK_NODE N ON (S.NETWORK_NODE_ID = N.ID) 
                WHERE S.RECORDOPENINGTIME >= '2018-04-05'
                AND S.RECORDOPENINGTIME < '2018-04-06'
                AND SERVEDMSISDN = '53453342'
                ) a
        group by SUSG_REF_NUM,RECORDOPENINGTIME,SERVEDMSISDN,CELL_NAME
        order by 6     
        ) b

where      
LATITUDE BETWEEN 59 and 60
and LONGITUDE BETWEEN 23 and 25
group by 
SERVEDMSISDN, 
SUSG_REF_NUM,  
CELL_NAME,
LATITUDE,
LONGITUDE,
RECORDOPENINGTIME
order by 1,2,3
;

WITH valim AS (
        SELECT
        SUM(lat0+lat1+lat2)/3 as LATITUDE,
        SUM(lon0+lon1+lon2)/3 as LONGITUDE,
        SUBSTR(CELL_NAME, 1, 5) as CELL_NAME,
        SUSG_REF_NUM,
        SERVEDMSISDN,
        RECORDOPENINGTIME
        from (
                SELECT
                S.SUSG_REF_NUM,
                S.RECORDOPENINGTIME,
                S.SERVEDMSISDN,
                N.CELL_NAME,
                N.LATITUDE as lat0,
                N.LONGITUDE as lon0,
                LEAD(N.LATITUDE,1,0) OVER (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lat1,
                LEAD(N.LONGITUDE,1,0) OVER (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lon1,
                LEAD(N.LATITUDE,2,0) OVER  (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lat2,
                LEAD(N.LONGITUDE,2,0) OVER (PARTITION BY S.SUSG_REF_NUM ORDER BY S.RECORDOPENINGTIME) as lon2

                FROM    DWH_PROD.SGSN_FACTS S
                INNER JOIN  DWH_PROD.NETWORK_NODE N ON (S.NETWORK_NODE_ID = N.ID) 
                /*INNER JOIN DWH_PROD.MV_BO_CURRENT_SUBSCRIBER subs ON subs.SUSG_REF = S.SUSG_REF_NUM
                                and subs.PACKAGE_CODE IN ('EMUL')  and subs.SUBS_STATUS = 'AC'*/
                WHERE S.RECORDOPENINGTIME >= '2018-03-01'
                AND S.RECORDOPENINGTIME < '2018-03-02'
                AND SERVEDMSISDN = '53453342'
                ) a
        group by SUSG_REF_NUM,RECORDOPENINGTIME,SERVEDMSISDN,CELL_NAME
        having SUM(lat0+lat1+lat2)/3 BETWEEN 59 and 60
        and SUM(lon0+lon1+lon2)/3 BETWEEN 23 and 25
        order by 6     
        )

select 
KP, CELL_NAME, count_cell, LATITUDE, LONGITUDE
from (
        select distinct
        cast(RECORDOPENINGTIME as date) as KP,
        v.SERVEDMSISDN,
        v.SUSG_REF_NUM,
        v.CELL_NAME,
        c.count_cell,
        AVG(LATITUDE)  as LATITUDE,
        AVG(LONGITUDE) as LONGITUDE 
        FROM valim v
        left join (
                select 
                count(CELL_NAME) as count_cell, SUSG_REF_NUM, CELL_NAME, cast(RECORDOPENINGTIME as date) as KP
                from valim v        
                group by 2,3,4
        )c on v.CELL_NAME = c.CELL_NAME and v.SUSG_REF_NUM = c.SUSG_REF_NUM and cast(v.RECORDOPENINGTIME as date) = c.KP
        group by 1,2,3,4,5
        order by 1,2,3,4
    )p




;

