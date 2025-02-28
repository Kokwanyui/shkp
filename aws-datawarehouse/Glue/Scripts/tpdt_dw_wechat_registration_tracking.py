import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime, date, time, timezone,timedelta
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import json
import time
from datetime import date

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'env'])
##Initialization
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.session.timeZone", "GMT+8")
spark.conf.set("spark.sql.broadcastTimeout", 7200)


##Input and Output Config
env = args['env']
table_name = args['JOB_NAME']
if env == 'prod':
    db01="tpdt_01replica"
    db02="tpdt_02replica"
    db03="tpdt_03dw"
    db03sub="tpdt_03sub"
    output_path="s3://tpdt-staging/"+table_name+"/"
    staging_path="s3://tpdt-staging/"
    dimension_path="s3://tpdt-dimension/"

else:
    db01="tpdt_01replica_"+env
    db03="tpdt_03dw_"+env
    db03sub="tpdt_03sub_"+env
    output_path="s3://tpdt-staging-"+env+"/"+table_name+"/"
    staging_path="s3://tpdt-staging-"+env+"/"
    dimension_path="s3://tpdt-dimension-"+env+"/"


prefix="shkpmalls_vip_"
prefix03='tpdt_'
datamart_prefix = "bi_datamart_"
dimension_prefix = "bi_dimension_"


##source table : dt_tracking_user_log
t_tracking_user_log_data = glueContext.create_dynamic_frame.from_catalog(database = db02, table_name = 'shkpmalls_wechat_analytics_dt_tracking_user_log', transformation_ctx = "tracking")
tracking_user_log_df = t_tracking_user_log_data.toDF()
tracking_user_log_df.createOrReplaceTempView("tracking_user_log_df")
print("shkpmalls_wechat_analytics_dt_tracking_user_log_extracted")


##source table : tpdt_dim_wechat_mall_mapping
t_tpdt_dim_wechat_mall_mapping = glueContext.create_dynamic_frame.from_catalog(database = db03sub, table_name = 'tpdt_dim_wechat_mall_mapping', transformation_ctx = "dim_mall")
tpdt_dim_wechat_mall_mapping_df = t_tpdt_dim_wechat_mall_mapping.toDF()
tpdt_dim_wechat_mall_mapping_df.createOrReplaceTempView("tpdt_dim_wechat_mall_mapping_df")
print("tpdt_dim_wechat_mall_mapping_extracted")

raw_table_df = spark.sql("""
   SELECT
     path
   , openid
   , last_track_time
   , scene
   , rank() OVER (PARTITION BY openid ORDER BY last_track_time ASC) ranking
   FROM tracking_user_log_df
   WHERE ((path IN ('pages/register/register4', 'pages/register/register_success')) AND ("year"(last_track_time) >= 2023))
""")
raw_table_df.createOrReplaceTempView("raw_table_df")

ranking_df =  spark.sql("""
   SELECT DISTINCT
     path
   , openid
   , last_track_time
   , scene
   FROM raw_table_df
   WHERE ranking = 1
""")
ranking_df.createOrReplaceTempView("ranking_df")

extracted_path_df = spark.sql("""
   SELECT
     path
   , openid
   , last_track_time
   , scene
   , (CASE
   WHEN (scene LIKE '{"path":"pages/boot_page","query":{"key":"%') THEN replace(replace(replace(CAST(json_extract(json_extract(scene, '$.query'), '$.key') AS varchar), '"', ''), 'key=', ''), 'key%3D', '') 
   WHEN (scene LIKE '{"path":"pages/boot_page","query":{"scene":"%') THEN replace(replace(replace(replace(replace(CAST(json_extract(json_extract(scene, '$.query'), '$.scene') AS varchar), '"', ''), 'key=', ''), 'key%3D', ''), 'oa%3Dbtl-', ''), 'btl-', '') 
   WHEN (scene LIKE '{"path":"pages/login","query":{"scene":"%') THEN replace(replace(replace(replace(replace(replace(CAST(json_extract(json_extract(scene, '$.query'), '$.scene') AS varchar), '"', ''), 'key=', ''), 'key%3D', ''), 'oa%3D', ''), 'btl-', ''), 'oa=', '') 
   WHEN (scene LIKE '{"path":"pages/login","query":{"oa":"%') THEN replace(replace(replace(CAST(json_extract(json_extract(scene, '$.query'), '$.oa') AS varchar), '"', ''), 'key=', ''), 'key%3D', '')
   ELSE 'OTHERS' END) extracted_mall_id
   FROM ranking_df
""")
extracted_path_df.createOrReplaceTempView("extracted_path_df")

final_df = spark.sql("""
SELECT
     path
   , openid
   , last_track_time
   , null as scene
   , extracted_mall_id
   , (CASE WHEN (mall IS NOT NULL) THEN mall ELSE 'OTHERS' END) finalised_mall_id
FROM extracted_path_df t1
LEFT JOIN (
   SELECT
     trackingpath
   , mall
   FROM tpdt_dim_wechat_mall_mapping_df
            )  t2
ON ("upper"(t1.extracted_mall_id) = "upper"(t2.trackingpath))
""")

# save
print("Saving Result into target destination...")
final_df_output_path="s3://tpdt-staging/tpdt_wechat_registration_tracking/"
final_df.write.format('parquet').mode('overwrite').option("header",True).save(final_df_output_path)
print(f"Result Saved in {final_df_output_path}...")



