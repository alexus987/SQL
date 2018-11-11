

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
                AND SERVEDMSISDN = 'phone_number'
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

