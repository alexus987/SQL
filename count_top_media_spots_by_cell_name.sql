--laadime meedia spotide koordinaadid sisse

CREATE TABLE media_plan_spots1 (geomeetria_centroid_x varchar(50), geomeetria_centroid_y varchar(50), object_name varchar(20));
TRUNCATE TABLE media_plan_spots_geo;
COPY media_plan_spots_geo FROM LOCAL '\\et\dfs\Grupitöö\CIA\Kliendi elukaare analüüsi grupp\Andmekaeve\Meedia planeerimine\koordinaadid_new.csv' DELIMITER ',' ENCLOSED BY '"' SKIP 1;
COPY media_plan_spots_geo FROM LOCAL '\\et\dfs\Grupitöö\CIA\Kliendi elukaare analüüsi grupp\Andmekaeve\Meedia planeerimine\koordinaadid_new_full.csv' DELIMITER ';' SKIP 1;
select * from media_plan_spots_geo order by object_name;

-- Mobiilse elu klientide positisoneerimine
-- loome tabeli alexsi.melu_valim 

drop table media_plan_spots1;
create table alexsi.melu_valim as 

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
                INNER JOIN DWH_PROD.MV_BO_CURRENT_SUBSCRIBER subs ON subs.SUSG_REF = S.SUSG_REF_NUM
                                and subs.PACKAGE_CODE IN ('EMUL')  and subs.SUBS_STATUS = 'AC'
                WHERE S.RECORDOPENINGTIME >= '2018-04-01'
                AND S.RECORDOPENINGTIME < '2018-04-02'
                
                ) a
        group by SUSG_REF_NUM,RECORDOPENINGTIME,SERVEDMSISDN,CELL_NAME
        having SUM(lat0+lat1+lat2)/3 BETWEEN 59 and 60
        and SUM(lon0+lon1+lon2)/3 BETWEEN 23 and 25
        order by 6     
        )
        
select 
M,
CELL_NAME,
count_cell,
LATITUDE as CELL_LAT,
LONGITUDE as CELL_LON,
s.*,
DISTANCE(LATITUDE, LONGITUDE, geomeetria_centroid_y, geomeetria_centroid_x) as DISTANCE
from (
        select distinct
        --cast(RECORDOPENINGTIME as date) as KP,
        extract('month' from RECORDOPENINGTIME) AS M,
        v.SERVEDMSISDN,
        v.SUSG_REF_NUM,
        v.CELL_NAME,
        c.count_cell,
        AVG(LATITUDE)  as LATITUDE,
        AVG(LONGITUDE) as LONGITUDE 
        FROM valim v
        left join (
                select 
                count(CELL_NAME) as count_cell, SUSG_REF_NUM, CELL_NAME, extract('month' from RECORDOPENINGTIME) as M
                from valim v        
                group by 2,3,4
        )c on v.CELL_NAME = c.CELL_NAME and v.SUSG_REF_NUM = c.SUSG_REF_NUM and extract('month' from v.RECORDOPENINGTIME) = c.M
        group by 1,2,3,4,5
        order by 1,2,3,4
    )p
cross join alexsi.media_plan_spots s
where 
DISTANCE(LATITUDE, LONGITUDE, geomeetria_centroid_y, geomeetria_centroid_x) < 0.5



/* analüüsime alexsi.melu_valim tabeli */

/* Lühike päring */

;
select distinct
v.object_name,
T05,
T02,
T01
from alexsi.melu_valim v
 left join (
        select  object_name, sum(count_cell) as T05
        from alexsi.melu_valim  
        where DISTANCE < 0.5 
        group by object_name
        ) D500 on v.object_name = D500.object_name
 left join (
        select  object_name, sum(count_cell) as T02
        from alexsi.melu_valim  
        where DISTANCE < 0.2 
        group by object_name
        ) D200 on v.object_name = D200.object_name
 left join (
        select  object_name, sum(count_cell) as T01
        from alexsi.melu_valim  
        where DISTANCE < 0.1 
        group by object_name
        ) D100 on v.object_name = D100.object_name
 
;



/* Pikk päring */
/* alexsi.melu_valim loomine ja analüüs ühes päringus*/

WITH OB AS (


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
                    INNER JOIN DWH_PROD.MV_BO_CURRENT_SUBSCRIBER subs ON subs.SUSG_REF = S.SUSG_REF_NUM
                                    and subs.PACKAGE_CODE IN ('EMUL')  and subs.SUBS_STATUS = 'AC'
                    WHERE S.RECORDOPENINGTIME >= '2018-06-01'
                    AND S.RECORDOPENINGTIME < '2018-06-02'
                
                    ) a
            group by SUSG_REF_NUM,RECORDOPENINGTIME,SERVEDMSISDN,CELL_NAME
            having SUM(lat0+lat1+lat2)/3 BETWEEN 59 and 60
            and SUM(lon0+lon1+lon2)/3 BETWEEN 23 and 25
            order by 6     
            )
        
    select 
    M,
    CELL_NAME,
    count_cell,
    LATITUDE as CELL_LAT,
    LONGITUDE as CELL_LON,
    s.*,
    DISTANCE(LATITUDE, LONGITUDE, geomeetria_centroid_y, geomeetria_centroid_x) as DISTANCE
    from (
            select distinct
            --cast(RECORDOPENINGTIME as date) as KP,
            extract('month' from RECORDOPENINGTIME) AS M,
            v.SERVEDMSISDN,
            v.SUSG_REF_NUM,
            v.CELL_NAME,
            c.count_cell,
            AVG(LATITUDE)  as LATITUDE,
            AVG(LONGITUDE) as LONGITUDE 
            FROM valim v
            left join (
                    select 
                    count(CELL_NAME) as count_cell, SUSG_REF_NUM, CELL_NAME, extract('month' from RECORDOPENINGTIME) as M
                    from valim v        
                    group by 2,3,4
            )c on v.CELL_NAME = c.CELL_NAME and v.SUSG_REF_NUM = c.SUSG_REF_NUM and extract('month' from v.RECORDOPENINGTIME) = c.M
            group by 1,2,3,4,5
            order by 1,2,3,4
        )p
    cross join ALEXSI.media_plan_spots_geo s
    where 
    DISTANCE(LATITUDE, LONGITUDE, geomeetria_centroid_y, geomeetria_centroid_x) < 1
 
)

select distinct
v.object_name,
v.geomeetria_centroid_x,
v.geomeetria_centroid_y,
v.address,
v.linnaosa,
v.Linn,
v.MK,
T1,
T05,
T02,
T01
from OB v
 left join (
        select  object_name, sum(count_cell) as T1
        from OB 
        where DISTANCE < 0.5 
        group by object_name
        ) D1000 on v.object_name = D1000.object_name
 left join (
        select  object_name, sum(count_cell) as T05
        from OB 
        where DISTANCE < 0.5 
        group by object_name
        ) D500 on v.object_name = D500.object_name
 left join (
        select  object_name, sum(count_cell) as T02
        from OB 
        where DISTANCE < 0.2 
        group by object_name
        ) D200 on v.object_name = D200.object_name
 left join (
        select  object_name, sum(count_cell) as T01
        from OB  
        where DISTANCE < 0.1 
        group by object_name
        ) D100 on v.object_name = D100.object_name