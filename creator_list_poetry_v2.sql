
WITH koo_source AS (

SELECT    
   ku.creator_id as creatorid,
   users.handle as koo_handle,
   min(date_format(from_unixtime(ku.created_at), '%Y-%m-%d')) as min_date,
   max(date_format(from_unixtime(ku.created_at), '%Y-%m-%d')) as max_date,
   COUNT(distinct (date_format(from_unixtime(ku.created_at), '%Y-%m-%d'))) as unique_days,  
   COUNT(distinct ( CASE WHEN ku.type=0 AND ku.parent_ku_id IS NULL THEN ku.id else null end)) as total_koos,
   imp.overall_impressions as overall_impressions,
   imp.overall_reactions as overall_reactions
  FROM "glue-postgre-data"."ku" ku
LEFT JOIN "glue-postgre-data"."users" users
ON (ku.creator_id= users.id) 
LEFT JOIN 
(
SELECT 
   creatorid,
   SUM(CASE WHEN eventname ='Content_Impression' THEN 1 ELSE 0 END) as overall_impressions,
   SUM(CASE WHEN eventname IN ('LikeKoo','ReKooWithComment','ReKooWithoutComment','ShareKoo')  THEN 1 ELSE 0 END) as overall_reactions  
FROM "koo-analytics-data-store"."koo_impressions" 
group by 1
) imp   
ON (ku.creator_id = imp.creatorid)  
WHERE date_format(from_unixtime(ku.created_at), '%Y-%m-%d')>='2021-12-05'
AND date_format(from_unixtime(ku.created_at), '%Y-%m-%d')<='2021-12-19'  
AND ku.identified_language=0 
GROUP BY 1,2,7,8
)

,classified_source as (
SELECT tk.creator_id as creatorid,
COUNT(DISTINCT  tk.koo_id) total_classified_koos,
COUNT(DISTINCT (CASE WHEN at.tag='Poems' then tk.koo_id  else null end)) as total_poems_koos,
sum(CASE WHEN at.tag='Poems'THEN imp.num_impressions ELSE NULL END) as num_poetry_impressions,
sum(CASE WHEN at.tag='Poems'THEN  imp.num_likes ELSE NULL END) as num_poetry_likes,
sum(CASE WHEN at.tag='Poems'THEN  imp.num_rekoos ELSE NULL END)as num_poetry_rekoos,
sum(CASE WHEN at.tag='Poems'THEN  imp.num_shares ELSE NULL END)as num_poetry_shares 
FROM "glue-postgre-data"."topic_koo_score" tk
LEFT JOIN
(
select distinct id,tag, name 
from "glue-postgre-data"."approved_topics" 
where visibility_status = 1 and topic_type < 2 
) at
ON (tk.topic_id=at.id)
LEFT JOIN 
(
SELECT creatorid,
contentid,  
SUM(CASE WHEN eventname ='Content_Impression'  THEN 1 ELSE 0 END) as num_impressions,
SUM(CASE WHEN eventname = 'LikeKoo'  THEN 1 ELSE 0 END) as num_likes,
SUM(CASE WHEN eventname IN ('ReKooWithComment','ReKooWithoutComment')  THEN 1 ELSE 0 END) as num_rekoos,
SUM(CASE WHEN eventname  = 'ShareKoo'  THEN 1 ELSE 0 END) as num_shares  
FROM "koo-analytics-data-store"."koo_impressions" 
group by 1,2  
) imp  
ON (tk.creator_id = imp.creatorid AND tk.koo_id = imp.contentid) 
WHERE  tk.identified_language=0 
AND date_format(from_unixtime(tk.created_at), '%Y-%m-%d')>='2021-12-05'
AND date_format(from_unixtime(tk.created_at), '%Y-%m-%d')<='2021-12-19'  
GROUP BY 1
)


SELECT 
ks.creatorid,
ks.koo_handle,
ks.min_date,
ks.max_date,
ks.unique_days,
ks.total_koos,
cs.total_classified_koos,
(ks.total_koos-cs.total_classified_koos) as total_unclassified_koos,
cs.total_poems_koos,
cs.num_poetry_impressions,
cs.num_poetry_likes,
cs.num_poetry_rekoos,
cs.num_poetry_shares,
ks.overall_impressions,
ks.overall_reactions,
ROUND(COALESCE((cs.total_classified_koos/CAST(NULLIF(ks.total_koos,0)as double)*100.0),0),4)  as classified_per,
ROUND(COALESCE(((ks.total_koos-cs.total_classified_koos)/CAST(NULLIF(ks.total_koos,0)as double)*100.0),0),4)  as unclassified_per,
ROUND(COALESCE((cs.total_poems_koos/CAST(NULLIF(ks.total_koos,0)as double)*100.0),0),4)  as poem_classified_per,
ROUND(CAST(COALESCE(((cs.num_poetry_likes+cs.num_poetry_rekoos+cs.num_poetry_shares)/CAST(NULLIF(cs.num_poetry_impressions,0)as double)),0) as double)*100.0 ,4)as rct_poem_per,
ROUND(COALESCE((ks.overall_reactions/CAST(NULLIF(ks.overall_impressions,0) as double)*100.0),0),4)  as overall_rct_per
FROM koo_source ks
LEFT JOIN classified_source cs
ON (ks.creatorid= cs.creatorid)