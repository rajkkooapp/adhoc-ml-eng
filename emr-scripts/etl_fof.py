#############################################################################################################
# ==========================================================================================================
# Date              Who               Description
# ==========   ================= =============================================================================
# 17/03/2022       Raj Kumar          Version 1 (notebooks to scripts)
##############################################################################################################
#   This program curates the Follow of Follow data for consideration set
#
#spark-submit  --master yarn --deploy-mode client  --conf spark.dynamicAllocation.enabled=true --conf spark.sql.shuffle.partitions=2001 --conf spark.default.parallelism=1000 etl_fof.py s3://aws-glue-neptune-data/postgre-data-lake/user_followings_list_v2/  s3://ml-test-analytics/test/stg_rfy/consideration_set/cs_users_20220308_160418.csv s3://ml-test-analytics/test/stg_rfy/stg_fof_test/ dev


    
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
    file_cs_users = argv[1]
    output_path = argv[2]
    run_env = argv[3]
    #repartition_num = int(argv[7])

    sparkAppName = "follow_of_follows"

    # log input fields
    logger = logging.getLogger(sparkAppName)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    logger.info("dir path user followings : " + dir_path_uf)
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

    user_followings_list = spark.read.format('parquet').load(file_path_uf).\
    select('user_id', 'following_ids','following_count')

    user_followings_list = user_followings_list.filter("following_count>0 AND following_count<=5000").\
    select('user_id', 'following_ids')

    cs_users_df = spark.read.csv(file_cs_users, header=True)
    cs_users_df = cs_users_df.select('user_id')

    cs_users_df = cs_users_df.withColumnRenamed("user_id","cs_user_id")
    cs_users_df = cs_users_df.withColumn("cs_user_id",cs_users_df["cs_user_id"].cast(IntegerType()))

    # Following list for Consideration set
    cs_ufl = cs_users_df.join(user_followings_list, 
                               cs_users_df.cs_user_id == user_followings_list.user_id,
                               "inner")

    cs_ufl = cs_ufl.drop(cs_ufl.user_id)

    # Exploding Once

    cs_ufl_exp = cs_ufl.select(cs_ufl.cs_user_id,explode(cs_ufl.following_ids).alias('cs_following_user_id'))

    # Getting follows of follows
    l1_following_list = cs_ufl_exp.join(user_followings_list, 
                                    cs_ufl_exp.cs_following_user_id == user_followings_list.user_id,
                                    "inner")
    l1_following_list = l1_following_list.drop(l1_following_list.user_id)

    # Exploding again

    l1_ufl_exp = l1_following_list.select(l1_following_list.cs_user_id,
                                      l1_following_list.cs_following_user_id, 
                                      explode(l1_following_list.following_ids).alias('l1_following_user_id'))
    # Filtering out invalid rows
    cond1 = (l1_ufl_exp.cs_user_id!=l1_ufl_exp.cs_following_user_id) # not following yourself
    cond2 = (l1_ufl_exp.cs_following_user_id!=l1_ufl_exp.l1_following_user_id) # cs_following not following themselves
    cond3 = (l1_ufl_exp.cs_user_id!=l1_ufl_exp.l1_following_user_id) # not suggesting oneself

    l1_ufl_exp = l1_ufl_exp.filter(cond1 & cond2 & cond3)


    final_counts_df = l1_ufl_exp.groupBy("cs_user_id", "l1_following_user_id").count()\
    .withColumnRenamed("count","fof_score").sort(desc("fof_score"))

    output_path_1 = output_path + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + "/dryrun/" 
    output_path_2 = output_path + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + "/dryrun-top100/" 


    # Filtering out already following

    fof_potential_df = final_counts_df.join(cs_ufl_exp,
                                        (final_counts_df.cs_user_id == cs_ufl_exp.cs_user_id) &
                                        (final_counts_df.l1_following_user_id == cs_ufl_exp.cs_following_user_id),
                                        how='left_anti')

    logger.info("Writing to output path 1 : " + output_path_1)



    fof_potential_df.write.parquet(output_path_1,mode="overwrite")


    window = Window.partitionBy(fof_potential_df['cs_user_id']).orderBy(fof_potential_df['fof_score'].desc())

    fof_ranked= fof_potential_df.select('*', row_number().over(window).alias('rank')).\
    filter(col('rank') <= 100).select('cs_user_id', 'l1_following_user_id','fof_score')    


    logger.info("Writing to output path 2 : " + output_path_2)

    fof_ranked.show()


    fof_ranked.write.parquet(output_path_2,mode="overwrite")  


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
