#############################################################################################################
# ==========================================================================================================
# Date              Who               Description
# ==========   ================= =============================================================================
# 17/03/2022       Raj Kumar          Version 1 (notebooks to scripts)
##############################################################################################################
#   This program curates the creators that follow you (Follow Back) data for consideration set
#
# spark-submit  --master yarn --deploy-mode client  --conf spark.dynamicAllocation.enabled=true --conf spark.sql.shuffle.partitions=2001 --conf spark.default.parallelism=1000 etl_follow_back.py s3://aws-glue-neptune-data/postgre-data-lake/user_followings_list_v2/ s3://aws-glue-neptune-data/postgre-data-lake/user_followers_list/ s3://aws-glue-neptune-data/postgre-data-lake/user_scores_log_v2/ s3://aws-glue-neptune-data/postgre-data-lake/users/ s3://ml-test-analytics/test/stg_rfy/consideration_set/cs_users_20220308_160418.csv s3://ml-test-analytics/test/stg_rfy/stg_fb_test/ dev


    
import datetime
import logging
import sys
import os
import configparser

# import necessary pyspark packages

try:
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from pyspark.sql.functions import desc
    from pyspark.sql.functions import explode
    import pyspark.sql.functions as F
    from pyspark.sql.types import IntegerType, StringType
    from pyspark.sql.window import Window
    from pyspark.sql.functions import rank, col,row_number
except ImportError as e:
    print("Error importing Spark Modules", e)
    sys.exit(1)


def main(argv):

    start_time = datetime.datetime.now()

    # assign input fields
    dir_path_uf = argv[0]
    dir_path_ufol = argv[1]
    dir_path_scores = argv[2]
    dir_path_users = argv[3]
    file_cs_users = argv[4]
    output_path = argv[5]
    run_env = argv[6]
    #repartition_num = int(argv[7])

    sparkAppName = "creators_that_follow_you"

    # log input fields
    logger = logging.getLogger(sparkAppName)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    logger.info("dir path user followings : " + dir_path_uf)
    logger.info("dir path user followers  : " + dir_path_ufol)
    logger.info("dir path people scores   : " + dir_path_scores)
    logger.info("dir path users           : " + dir_path_users)
    logger.info("considertaion set users  : " + file_cs_users)
    logger.info("output path : " + output_path)
    logger.info("Run Environment : " + run_env)

    # Setting up Spark Session
    spark = SparkSession.builder.appName(sparkAppName).enableHiveSupport().config(
        "spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true").config("hive.mapred.supports.subdirectories", "true").getOrCreate()

    # initialize sparkcontext and sqlcontext
    sc = spark.sparkContext
    spark = SparkSession(sc)


    # Parameters
    partition_dt = datetime.datetime.today().date()

    # dir_path_ufl= s3://aws-glue-neptune-data/postgre-data-lake/user_followings_list/

    file_path_uf = dir_path_uf +"partition="+str(partition_dt)+"/"

    # Reading User followings

    user_followings_list = spark.read.format('parquet').load(file_path_uf).\
    select('user_id', 'following_ids','following_count')
    
    user_followings_list = user_followings_list.filter("following_count>0 AND following_count<=300").\
    select('user_id', 'following_ids')

    # Reading User followers
    #dir_path_ufol = "s3://aws-glue-neptune-data/postgre-data-lake/user_followers_list/"
    file_path_ufollowers = dir_path_ufol+"partition="+str(partition_dt)+"/"
    user_followers_list = spark.read.format('parquet').load(file_path_ufollowers).\
    select('user_id', 'follower_ids')

    # Reading Users table

    #dir_path_users = s3://aws-glue-neptune-data/postgre-data-lake/users/

    file_path_users= dir_path_users+"partition="+str(partition_dt)+"/"
    users = spark.read.format('parquet').load(file_path_users).\
    select('id', 'current_language')
    users= users.withColumnRenamed("id","user_id")

    #People Score

    #dir_path_scores = "s3://aws-glue-neptune-data/postgre-data-lake/user_scores_log_v2/"
    file_path_scores= dir_path_scores+"partition="+str(partition_dt)+"/"
    people_score = spark.read.format('parquet').load(file_path_scores).\
    select('user_id','language' ,'final_score')

    #Consideration set

    #file_path_users= "s3://ml-test-analytics/test/stg_rfy/consideration_set/cs_users_20220203_173818.csv"
    
    cs_users_df = spark.read.csv(file_cs_users, header=True)
    cs_users_df = cs_users_df.select('user_id')

    cs_users_df = cs_users_df.withColumnRenamed("user_id","cs_user_id")
    cs_users_df = cs_users_df.withColumn("cs_user_id",cs_users_df["cs_user_id"].cast(IntegerType()))

    #Following list consideration set

    cs_ufl = cs_users_df.join(user_followings_list, 
                               cs_users_df.cs_user_id == user_followings_list.user_id,
                               "inner")

    # Exploding Users following

    cs_ufl_exp = cs_ufl.select(cs_ufl.cs_user_id,explode(cs_ufl.following_ids).alias('cs_following_user_id'))


    # Followers list consideration set 
    cs_ufol = cs_users_df.join(user_followers_list, 
                               cs_users_df.cs_user_id == user_followers_list.user_id,
                               "inner")


    # Exploding Users followers

    cs_ufol_exp = cs_ufol.select(cs_ufol.cs_user_id,explode(cs_ufol.follower_ids).alias('cs_follower_user_id'))

    #Subtracting user's followers from the user's following, is what they can follow back

    fb_potential_df =cs_ufol_exp.subtract(cs_ufl_exp)

    # Getting the user id language
    fb_potential_df = fb_potential_df.join(users, 
                                   fb_potential_df.cs_user_id == users.user_id,
                                   "left").select(fb_potential_df['cs_user_id'],fb_potential_df['cs_follower_user_id'],users['current_language'].alias('cl_cs_user_id'))
    # Getting the follower id language
    fb_potential_df = fb_potential_df.join(users, 
                                   fb_potential_df.cs_follower_user_id == users.user_id,
                                   "left").select(fb_potential_df['cs_user_id'],fb_potential_df['cs_follower_user_id'],fb_potential_df['cl_cs_user_id'],users['current_language'].alias('cl_cs_follower_user_id'))

    # Filter Recommendations where language is not matching
    fb_potential_df= fb_potential_df.filter("cl_cs_user_id=cl_cs_follower_user_id")


    fb_potential_df  = fb_potential_df .join(people_score,
                                        (fb_potential_df.cs_follower_user_id == people_score.user_id) &
                                        (fb_potential_df.cl_cs_follower_user_id == people_score.language),
                                        how='left').\
    select(fb_potential_df['cs_user_id'],fb_potential_df['cs_follower_user_id'], people_score['final_score']).\
    filter("final_score>=2.0")

    output_path_1 = output_path + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + "/dryrun/" 
    output_path_2 = output_path + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + "/dryrun-top100/" 

    logger.info("Writing to output path : " + output_path_1)

    fb_potential_df.write.parquet(output_path_1,mode="overwrite")



    window = Window.partitionBy(fb_potential_df['cs_user_id']).orderBy(fb_potential_df['final_score'].desc())
    fb_ranked= fb_potential_df.select('*', row_number().over(window).alias('rank')).\
      filter(col('rank') <= 100).select('cs_user_id', 'cs_follower_user_id','final_score')

    logger.info("Writing to output path : " + output_path_2)

    fb_ranked.write.parquet(output_path_2,mode="overwrite")
  

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
