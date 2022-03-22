#############################################################################################################
# ==========================================================================================================
# Date              Who               Description
# ==========   ================= =============================================================================
# 17/03/2022       Raj Kumar          Version 1 (notebooks to scripts)
##############################################################################################################
#   This program curates the Profile Views data for consideration set
#
#spark-submit  --master yarn --deploy-mode client  --conf spark.dynamicAllocation.enabled=true --conf spark.sql.shuffle.partitions=2001 --conf spark.default.parallelism=1000 etl_pr_views.py s3://koo-data-lake-prod/Koo-impressions/  s3://aws-glue-neptune-data/postgre-data-lake/user_followings_list_v2/  s3://aws-glue-neptune-data/postgre-data-lake/user_scores_log_v2/ s3://ml-test-analytics/test/stg_rfy/consideration_set/cs_users_20220308_160418.csv s3://ml-test-analytics/test/stg_rfy_test/ 3 100


    
import datetime
import logging
import sys
import os
import configparser

# import necessary pyspark packages

try:
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from pyspark.sql import functions as f
    from pyspark.sql import types as t
    from pyspark.sql.functions import *
    from pyspark.sql.window import Window
    from pyspark.sql.functions import rank, col,row_number
except ImportError as e:
    print("Error importing Spark Modules", e)
    sys.exit(1)


def main(argv):

    start_time = datetime.datetime.now()


    # assign input fields
    dir_path_imp = argv[0]
    dir_path_uf = argv[1]
    dir_path_ps = argv[2]
    file_cs_users = argv[3]
    output_path = argv[4]
    min_profile_views = argv[5]
    max_rank = argv[6]
    #repartition_num = int(argv[7])

    sparkAppName = "profile_views"

    # log input fields
    logger = logging.getLogger(sparkAppName)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    logger.info("dir path raw_impressions : " + dir_path_imp)
    logger.info("dir path user followings : " + dir_path_uf)
    logger.info("dir path people_score : " + dir_path_ps)
    logger.info("considertaion set users  : " + file_cs_users)
    logger.info("output path : " + output_path)
    logger.info("min profile_views : " + min_profile_views)
    logger.info("min profile_views : " + max_rank)

    # Setting up Spark Session
    spark = SparkSession.builder.appName(sparkAppName).enableHiveSupport().config(
        "spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true").config("hive.mapred.supports.subdirectories", "true").getOrCreate()

    # initialize sparkcontext and sqlcontext
    sc = spark.sparkContext
    spark = SparkSession(sc)


    # Parameters
    partition_dt= datetime.datetime.today().date()
    dt= datetime.datetime.today()  - datetime.timedelta(days=1)

    days_to_load = range(0, 90)

    filepaths = []
    for days_to_subtract in days_to_load:
        d = dt - datetime.timedelta(days_to_subtract)
        filepath = dir_path_imp + 'year=' + str(
            d.year) + '/month=' + format(d.month, '02d') + '/day=' + format(
                d.day, '02d') + "/*/*"
        filepaths.append(filepath)

    logger.info("raw_impressions file path :")
    logger.info(filepaths)
    


    # Reading Impressions Data
    df_imp = spark.read.format('parquet').load(filepaths).select('timestamp','creatorid','userId','eventname','screen')
    df_imp.registerTempTable("raw_impressions")    

    # Reading user followings data
    file_path_uf = dir_path_uf +"partition="+str(partition_dt)+"/"
    
    logger.info("Reading user following path : " + file_path_uf)


    df_uf = spark.read.format('parquet').load(file_path_uf).select('user_id','following_ids')
    df_uf = df_uf.withColumn("following_ids",
       concat_ws(",",col("following_ids")))
    df_uf.registerTempTable("user_followings")



    # Reading People Score
    #dir_path_ps='s3://aws-glue-neptune-data/postgre-data-lake/user_scores_log_v2/'
    file_path_ps =  dir_path_ps + 'partition=' +str(partition_dt)+"/"
    df_ps = spark.read.format('parquet').load(file_path_ps).select('user_id','language','final_score')
    df_ps.registerTempTable("people_score")  

    # Reading consideration set 
    cs_user_id = spark.read.format("csv").option("header", "true").load(file_cs_users)
    cs_user_id_list = cs_user_id.select('user_id').rdd.flatMap(lambda x: x).collect()

    query= """WITH profile_visits AS
    (
    SELECT userid as user_id, creatorid as creator_id, COUNT(DISTINCT timestamp) as profile_views
    FROM raw_impressions
    WHERE userid IN {cs_user_id_list}
    AND eventname IN ('UserProfileView') AND screen='UserProfile'
    AND creatorid NOT IN (-1,0)
    GROUP BY 1,2
    ),user_followings AS
    (
    SELECT user_id, following_ids
    FROM user_followings
    WHERE user_id IN {cs_user_id_list}
    ),uf_profile_visits AS 
    (
    SELECT 
    user_id,
    uf.following_ids,
    pv.creator_id,
    pv.profile_views
    FROM profile_visits pv
    INNER JOIN user_followings uf
    USING (user_id) 
    WHERE pv.profile_views>={min_profile_views}
    )
    SELECT * FROM uf_profile_visits
    """.format(cs_user_id_list=tuple(cs_user_id_list),min_profile_views=min_profile_views)
    
    logger.info("Query 1 : " + query)


    profile_visits =spark.sql(query)

    logger.info("Running Transformation for following")

    df_pr_views = profile_visits.withColumn(
        'is_following',
        f.when(col("following_ids").contains(col("creator_id")), 1)\
        .otherwise(0)
    ).filter("is_following==0").drop("is_following","following_ids")
    
    df_pr_views.registerTempTable("unfollowed_profile_visits")

    query="""
    WITH people_score AS
    ( 
      SELECT user_id as creator_id,
      max(final_score) as people_score
      FROM people_score
      WHERE user_id IN (SELECT creator_id FROM unfollowed_profile_visits)
      GROUP BY 1
    ),ranked_profiles AS
    (
      SELECT 
      pv.user_id, 
      creator_id,
      pv.profile_views,
      ps.people_score,
      row_number()
            OVER (PARTITION BY pv.user_id ORDER BY pv.profile_views DESC, ps.people_score DESC) AS rk
      FROM unfollowed_profile_visits pv
      LEFT JOIN people_score ps
      USING (creator_id)
    )

    SELECT *
    FROM ranked_profiles 
    WHERE rk<={max_rank}
    ORDER BY user_id, rk""".format(max_rank=max_rank)

    logger.info("Query 2 : " + query)


    profile_visits_df = spark.sql(query)

    output_path_1 = output_path +"profile_visits_" +datetime.datetime.now().strftime("%Y%m%d_%H%M%S")+".csv"
    output_path_2 = output_path + "pr_view_signal_"+datetime.datetime.now().strftime("%Y%m%d_%H%M%S")+".csv" 


    logger.info("Profile visits data generated part 1: " + output_path_1)


    profile_visits_df.coalesce(1).write.option("header","true").option("sep",",").mode("overwrite").csv(output_path_1)

    window_var = Window().partitionBy('user_id')
    profile_visits_df = profile_visits_df.withColumn('signal_val', round(f.col('profile_views')/f.max('profile_views').over(window_var),4))
    profile_visits_df = profile_visits_df.withColumn("signal", lit('pr_view')) 

    logger.info("Profile visits data generated part 2: " + output_path_2)
    profile_visits_df.coalesce(1).write.option("header","true").option("sep",",").mode("overwrite").csv(output_path_2)


    end_time = datetime.datetime.now()
    duration = end_time - start_time
    logger.info('total duration: ' + str(duration))
    spark.stop()


if __name__ == "__main__":
    logging.info(len(sys.argv))
    if len(sys.argv) < 1:
        raise Exception('Incorrect number of arguments passed')

    logging.info('Number of arguments:', len(sys.argv), 'arguments.')
    logging.info('Argument List:', str(sys.argv))
    main(sys.argv[1:])
