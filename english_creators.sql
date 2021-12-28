# English Creators

#Entertainment

WITH koo_source AS (

SELECT 
   
   ku.creator_id as creatorid,
   users.handle as koo_handle, 
   COUNT(distinct ( CASE WHEN ku.type=0 AND ku.parent_ku_id IS NULL THEN ku.id else null end)) as total_koos
FROM "glue-postgre-data"."ku" ku
LEFT JOIN "glue-postgre-data"."users" users
ON (ku.creator_id= users.id) 
WHERE date_format(from_unixtime(ku.created_at), '%Y-%m-%d')>='2021-12-02'
AND date_format(from_unixtime(ku.created_at), '%Y-%m-%d')<='2021-12-19'  
AND ku.identified_language=50 
GROUP BY 1,2
) ,classified_source as (

SELECT 
tk.creator_id as creatorid,
COUNT(distinct(CASE WHEN at.tag='Entertainment' then tk.koo_id  else null end)) as total_entertainment_koos
FROM "glue-postgre-data"."topic_koo_score" tk
LEFT JOIN
(
select distinct id,tag, name 
from "glue-postgre-data"."approved_topics" 
where visibility_status = 1 and topic_type < 2 
) at
ON (tk.topic_id=at.id)
WHERE tk.identified_language=50
GROUP BY 1 
)


SELECT 
ks.creatorid,
ks.koo_handle,
ks.total_koos,
cs.total_entertainment_koos,
ROUND(COALESCE((cs.total_entertainment_koos/CAST(NULLIF(ks.total_koos,0)as double)*100.0),0),4) as percentage_classified
FROM koo_source ks
LEFT JOIN classified_source cs
ON (ks.creatorid= cs.creatorid)
WHERE cs.total_entertainment_koos>=5
ORDER BY cs.total_entertainment_koos desc

#Religious

WITH koo_source AS (

SELECT 
   
   ku.creator_id as creatorid,
   users.handle as koo_handle, 
   COUNT(distinct ( CASE WHEN ku.type=0 AND ku.parent_ku_id IS NULL THEN ku.id else null end)) as total_koos
FROM "glue-postgre-data"."ku" ku
LEFT JOIN "glue-postgre-data"."users" users
ON (ku.creator_id= users.id) 
WHERE date_format(from_unixtime(ku.created_at), '%Y-%m-%d')>='2021-12-02'
AND date_format(from_unixtime(ku.created_at), '%Y-%m-%d')<='2021-12-19'  
AND ku.identified_language=50 
GROUP BY 1,2
) ,classified_source as (

SELECT 
tk.creator_id as creatorid,
COUNT(distinct(CASE WHEN at.tag='Religious' then tk.koo_id  else null end)) as total_religious_koos
FROM "glue-postgre-data"."topic_koo_score" tk
LEFT JOIN
(
select distinct id,tag, name 
from "glue-postgre-data"."approved_topics" 
where visibility_status = 1 and topic_type < 2 
) at
ON (tk.topic_id=at.id)
WHERE tk.identified_language=50
GROUP BY 1 
)


SELECT 
ks.creatorid,
ks.koo_handle,
ks.total_koos,
cs.total_religious_koos,
ROUND(COALESCE((cs.total_religious_koos/CAST(NULLIF(ks.total_koos,0)as double)*100.0),0),4) as percentage_classified
FROM koo_source ks
LEFT JOIN classified_source cs
ON (ks.creatorid= cs.creatorid)
WHERE cs.total_religious_koos>=5
ORDER BY cs.total_religious_koos desc

#Sports

WITH koo_source AS (

SELECT 
   
   ku.creator_id as creatorid,
   users.handle as koo_handle, 
   COUNT(distinct ( CASE WHEN ku.type=0 AND ku.parent_ku_id IS NULL THEN ku.id else null end)) as total_koos
FROM "glue-postgre-data"."ku" ku
LEFT JOIN "glue-postgre-data"."users" users
ON (ku.creator_id= users.id) 
WHERE date_format(from_unixtime(ku.created_at), '%Y-%m-%d')>='2021-12-02'
AND date_format(from_unixtime(ku.created_at), '%Y-%m-%d')<='2021-12-19'  
AND ku.identified_language=50 
GROUP BY 1,2
) ,classified_source as (

SELECT 
tk.creator_id as creatorid,
COUNT(distinct(CASE WHEN at.tag='Sports' then tk.koo_id  else null end)) as total_sports_koos
FROM "glue-postgre-data"."topic_koo_score" tk
LEFT JOIN
(
select distinct id,tag, name 
from "glue-postgre-data"."approved_topics" 
where visibility_status = 1 and topic_type < 2 
) at
ON (tk.topic_id=at.id)
WHERE tk.identified_language=50
GROUP BY 1 
)


SELECT 
ks.creatorid,
ks.koo_handle,
ks.total_koos,
cs.total_sports_koos,
ROUND(COALESCE((cs.total_sports_koos/CAST(NULLIF(ks.total_koos,0)as double)*100.0),0),4) as percentage_classified
FROM koo_source ks
LEFT JOIN classified_source cs
ON (ks.creatorid= cs.creatorid)
WHERE cs.total_sports_koos>=5
ORDER BY cs.total_sports_koos desc

#Education

WITH koo_source AS (

SELECT 
   
   ku.creator_id as creatorid,
   users.handle as koo_handle, 
   COUNT(distinct ( CASE WHEN ku.type=0 AND ku.parent_ku_id IS NULL THEN ku.id else null end)) as total_koos
FROM "glue-postgre-data"."ku" ku
LEFT JOIN "glue-postgre-data"."users" users
ON (ku.creator_id= users.id) 
WHERE date_format(from_unixtime(ku.created_at), '%Y-%m-%d')>='2021-12-02'
AND date_format(from_unixtime(ku.created_at), '%Y-%m-%d')<='2021-12-19'  
AND ku.identified_language=50 
GROUP BY 1,2
) ,classified_source as (

SELECT 
tk.creator_id as creatorid,
COUNT(distinct(CASE WHEN at.tag='Education' then tk.koo_id  else null end)) as total_education_koos
FROM "glue-postgre-data"."topic_koo_score" tk
LEFT JOIN
(
select distinct id,tag, name 
from "glue-postgre-data"."approved_topics" 
where visibility_status = 1 and topic_type < 2 
) at
ON (tk.topic_id=at.id)
WHERE tk.identified_language=50
GROUP BY 1 
)


SELECT 
ks.creatorid,
ks.koo_handle,
ks.total_koos,
cs.total_education_koos,
ROUND(COALESCE((cs.total_education_koos/CAST(NULLIF(ks.total_koos,0)as double)*100.0),0),4) as percentage_classified
FROM koo_source ks
LEFT JOIN classified_source cs
ON (ks.creatorid= cs.creatorid)
WHERE cs.total_education_koos>=5
ORDER BY cs.total_education_koos desc

#Health

WITH koo_source AS (

SELECT 
   
   ku.creator_id as creatorid,
   users.handle as koo_handle, 
   COUNT(distinct ( CASE WHEN ku.type=0 AND ku.parent_ku_id IS NULL THEN ku.id else null end)) as total_koos
FROM "glue-postgre-data"."ku" ku
LEFT JOIN "glue-postgre-data"."users" users
ON (ku.creator_id= users.id) 
WHERE date_format(from_unixtime(ku.created_at), '%Y-%m-%d')>='2021-12-02'
AND date_format(from_unixtime(ku.created_at), '%Y-%m-%d')<='2021-12-19'  
AND ku.identified_language=50 
GROUP BY 1,2
) ,classified_source as (

SELECT 
tk.creator_id as creatorid,
COUNT(distinct(CASE WHEN at.tag='Health' then tk.koo_id  else null end)) as total_health_koos
FROM "glue-postgre-data"."topic_koo_score" tk
LEFT JOIN
(
select distinct id,tag, name 
from "glue-postgre-data"."approved_topics" 
where visibility_status = 1 and topic_type < 2 
) at
ON (tk.topic_id=at.id)
WHERE tk.identified_language=50
GROUP BY 1 
)


SELECT 
ks.creatorid,
ks.koo_handle,
ks.total_koos,
cs.total_health_koos,
ROUND(COALESCE((cs.total_health_koos/CAST(NULLIF(ks.total_koos,0)as double)*100.0),0),4) as percentage_classified
FROM koo_source ks
LEFT JOIN classified_source cs
ON (ks.creatorid= cs.creatorid)
WHERE cs.total_health_koos>=5
ORDER BY cs.total_health_koos desc



#Politics

WITH koo_source AS (

SELECT 
   
   ku.creator_id as creatorid,
   users.handle as koo_handle, 
   COUNT(distinct ( CASE WHEN ku.type=0 AND ku.parent_ku_id IS NULL THEN ku.id else null end)) as total_koos
FROM "glue-postgre-data"."ku" ku
LEFT JOIN "glue-postgre-data"."users" users
ON (ku.creator_id= users.id) 
WHERE date_format(from_unixtime(ku.created_at), '%Y-%m-%d')>='2021-12-02'
AND date_format(from_unixtime(ku.created_at), '%Y-%m-%d')<='2021-12-19'  
AND ku.identified_language=50 
GROUP BY 1,2
) ,classified_source as (

SELECT 
tk.creator_id as creatorid,
COUNT(distinct(CASE WHEN at.tag='Politics' then tk.koo_id  else null end)) as total_politics_koos
FROM "glue-postgre-data"."topic_koo_score" tk
LEFT JOIN
(
select distinct id,tag, name 
from "glue-postgre-data"."approved_topics" 
where visibility_status = 1 and topic_type < 2 
) at
ON (tk.topic_id=at.id)
WHERE tk.identified_language=50
GROUP BY 1 
)


SELECT 
ks.creatorid,
ks.koo_handle,
ks.total_koos,
cs.total_politics_koos,
ROUND(COALESCE((cs.total_politics_koos/CAST(NULLIF(ks.total_koos,0)as double)*100.0),0),4) as percentage_classified
FROM koo_source ks
LEFT JOIN classified_source cs
ON (ks.creatorid= cs.creatorid)
WHERE cs.total_politics_koos>=5
ORDER BY cs.total_politics_koos desc


# Business
WITH koo_source AS (

SELECT 
   
   ku.creator_id as creatorid,
   users.handle as koo_handle, 
   COUNT(distinct ( CASE WHEN ku.type=0 AND ku.parent_ku_id IS NULL THEN ku.id else null end)) as total_koos
FROM "glue-postgre-data"."ku" ku
LEFT JOIN "glue-postgre-data"."users" users
ON (ku.creator_id= users.id) 
WHERE date_format(from_unixtime(ku.created_at), '%Y-%m-%d')>='2021-12-02'
AND date_format(from_unixtime(ku.created_at), '%Y-%m-%d')<='2021-12-19'  
AND ku.identified_language=50 
GROUP BY 1,2
) ,classified_source as (

SELECT 
tk.creator_id as creatorid,
COUNT(distinct(CASE WHEN at.tag='Business' then tk.koo_id  else null end)) as total_business_koos
FROM "glue-postgre-data"."topic_koo_score" tk
LEFT JOIN
(
select distinct id,tag, name 
from "glue-postgre-data"."approved_topics" 
where visibility_status = 1 and topic_type < 2 
) at
ON (tk.topic_id=at.id)
WHERE tk.identified_language=50
GROUP BY 1 
)


SELECT 
ks.creatorid,
ks.koo_handle,
ks.total_koos,
cs.total_business_koos,
ROUND(COALESCE((cs.total_business_koos/CAST(NULLIF(ks.total_koos,0)as double)*100.0),0),4) as percentage_classified
FROM koo_source ks
LEFT JOIN classified_source cs
ON (ks.creatorid= cs.creatorid)
WHERE cs.total_business_koos>=5
ORDER BY cs.total_business_koos desc