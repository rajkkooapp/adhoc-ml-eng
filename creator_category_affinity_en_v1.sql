SELECT 
   ku.creator_id as creatorid,
   users.handle as koo_handle,
   min(date_format(from_unixtime(ku.created_at), '%Y-%m-%d')) as min_date,
   max(date_format(from_unixtime(ku.created_at), '%Y-%m-%d')) as max_date,
   COUNT(distinct (date_format(from_unixtime(ku.created_at/1000), '%Y-%m-%d'))) as unique_days,  
   COUNT(distinct ku.id) as total_koos,
   COUNT(distinct(CASE WHEN md.sub_category NOT IN (14,13,17,30,16,12,11)  then ku.id else null end)) as total_koos_unclassified,   
   COUNT(distinct(CASE WHEN md.sub_category = 14 then ku.id  else null end)) as total_koos_entertainment,
   COUNT(distinct(CASE WHEN md.sub_category = 13 then ku.id  else null end)) as total_koos_religious,
   COUNT(distinct(CASE WHEN md.sub_category = 17 then ku.id  else null end)) as total_koos_sports, 
   COUNT(distinct(CASE WHEN md.sub_category = 30 then ku.id   else null end)) as total_koos_education, 
   COUNT(distinct(CASE WHEN md.sub_category = 16 then ku.id   else null end)) as total_koos_health, 
   COUNT(distinct(CASE WHEN md.sub_category = 12 then ku.id   else null end)) as total_koos_business,         
   COUNT(distinct(CASE WHEN md.sub_category = 11    then ku.id   else null end)) as total_koos_politics   
FROM "glue-postgre-data"."ku" ku
LEFT JOIN "glue-postgre-data"."koo_metadata" md
ON (ku.id=md.koo_id)
LEFT JOIN "glue-postgre-data"."users" users
ON (ku.creator_id= users.id) 
WHERE date_format(from_unixtime(ku.created_at), '%Y-%m-%d')>='2021-12-05'
AND date_format(from_unixtime(ku.created_at), '%Y-%m-%d')<='2021-12-20'
AND ku.identified_language=50
GROUP BY 1,2