-- top k english creators in key categories
SELECT creatorid,
         kooid,
         category,
         num_impressions,
         num_reactions,
        rk_imp
FROM 
    (SELECT creatorid,
         kooid,
         category,
         num_impressions,
         num_reactions,
         row_number()
        OVER (PARTITION BY category
    ORDER BY  num_impressions DESC) AS rk_imp
    FROM 
        (SELECT imp.creatorid AS creatorid,
         imp.contentid AS kooid,
        
            CASE
            WHEN md.sub_category IN (14,13,17,30,16,12,11) THEN
            md.sub_category
            ELSE 0
            END AS category, SUM(CASE
            WHEN imp.eventname='Content_Impression' THEN
            1
            ELSE 0 END) AS num_impressions, SUM(CASE
            WHEN imp.eventname IN ('ReplyKoo', 'LikeKoo','ReKooWithComment','ReKooWithoutComment','ShareKoo','PlayedKoo') THEN
            1
            ELSE 0 END) AS num_reactions
        FROM "koo-analytics-data-store"."koo_impressions" imp
        LEFT JOIN "glue-postgre-data"."koo_metadata" md
            ON (imp.contentid=md.koo_id)
        WHERE split_part(imp.partition_dt,' ',1)>='2021-12-20'
                AND split_part(imp.partition_dt,' ',1)<='2021-12-20'
                AND imp.language IN ('en')
                AND imp.screen='Feed'
        GROUP BY  1,2,3 ) )
    WHERE rk_imp<=5 