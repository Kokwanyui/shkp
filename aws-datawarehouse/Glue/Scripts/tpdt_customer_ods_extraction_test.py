import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime, date, time, timezone,timedelta 
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import json
from datetime import datetime, date, time, timezone,timedelta 


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'env'])
##Initialization
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


##Input and Output Config
env = args['env']
table_name = args['JOB_NAME']
if env == 'prod':
    db01="tpdt_01replica"
    db03="tpdt_03dw"
    db02="tpdt_02replica"
    staging_path="s3://tpdt-staging/"
    dimension_path="s3://tpdt-dimension/"
else:
    db01="tpdt_01replica_"+env
    db03="tpdt_03dw_"+env
    staging_path="s3://tpdt-staging-"+env+"/"
    dimension_path="s3://tpdt-dimension-"+env+"/"


prefix="shkpmalls_vip_"


## Source Extraction
##source table : member_activity_history------------------------------------------
t_member_activity_history = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = prefix+'member_activity_history', transformation_ctx = "member_activity_history")
member_activity_history_df = t_member_activity_history.toDF().withColumn("action_type_detail", when(col("action_type")==1, "Login")\
                                                          .when(col("action_type")==2, "Change Status")\
                                                          .when(col("action_type")==3, "Change Password")\
                                                          .when(col("action_type")==4, "Forget Password")\
                                                          .when(col("action_type")==5, "Verified")\
                                                          .when(col("action_type")==6, "Register")\
                                                          .when(col("action_type")==7, "Reset Password")\
                                                          .otherwise(""))\
                                                          .withColumn("ip_address", lit(""))
spark.conf.set("spark.sql.session.timeZone", "UTC")
member_activity_history_output_path = "s3://tpdt-dw/tpdt_ods_member_activity_history/"                                            
member_activity_history_df.write.format('parquet').partitionBy("action_type").mode('overwrite').option("header",True).save(member_activity_history_output_path)






##source table : partner_member_mapping---------------------------------------------
t_partner_member_mapping = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = prefix+'partner_member_mapping', transformation_ctx = "member_activity_history")
partner_member_mapping_df = t_partner_member_mapping.toDF()
partner_member_mapping_df.createOrReplaceTempView("ods_partner_member_mapping")
print("Source Extraxtion Finished: Partner_member_mapping...")

##source table : member
t_member = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = prefix+'member', transformation_ctx = "member")
member_df = t_member.toDF()
member_df.createOrReplaceTempView("member")
spark.conf.set("spark.sql.session.timeZone", "UTC")


## Data Transformation
spark.conf.set("spark.sql.session.timeZone", "GMT+8")
print("Perform data transformation...")

ods_partner_member_mapping_v2 = spark.sql("""
    select  
        t1.mapping_id,
        t1.created_by,
        t1.created_date,
        t1.status,
        t1.updated_by,
        t1.updated_date,
        t2.member_id,
        t1.partner_member_id,
        -- t1.phone,
        t1.phone_prefix,
        t1.app_id
    From ods_partner_member_mapping t1 
    left join member t2 on t1.phone= t2.phone
""")
member_activity_history_output_path = "s3://tpdt-dw/tpdt_ods_partner_member_mapping/"    
ods_partner_member_mapping_v2.write.format('parquet').partitionBy("app_id").mode('overwrite').option("header",True).save(member_activity_history_output_path)






## Source Extraction
##source table : device_info-----------------------------------------------------
t_device_info = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_push_device_info', transformation_ctx = "device_info")
device_info_df = t_device_info.toDF()
spark.conf.set("spark.sql.session.timeZone", "UTC")
hk_time = datetime.now() - timedelta(hours=16)
ytd = hk_time.date().strftime('%Y-%m-%d')

delta = device_info_df.filter(f"to_date(updated_date) = '{ytd}' or to_date(created_date) = '{ytd}'")\
                      .withColumn("sim_card_country_code",col("sim_card_country_code").cast(StringType()))\
                      .withColumn("sim_card_serial_number",col("sim_card_serial_number").cast(StringType()))

## Loading Result to S3
device_info_output_path = "s3://tpdt-dw/tpdt_ods_device_info/"  
device_info_df.write.format('parquet').partitionBy("device_os_version").mode('overwrite').option("header",True).save(device_info_output_path)
#delta.write.format('parquet').mode('append').option("header",True).save('s3://tpdt-staging/tpdt_log_member_device/')







##source table : tpdt_dw_sso_mapping -----------------------------------------------------------------------------------------------------------------------------------------------------------
##source table : mapping
t_mapping = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_sso_mapping', transformation_ctx = "mapping")
mapping_df = t_mapping.toDF()
mapping_df.createOrReplaceTempView("mapping")
print("Source Extraxtion Finished: mapping...")

##source table : application
t_application = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_sso_application', transformation_ctx = "application")
application_df = t_application.toDF()
application_df.createOrReplaceTempView("application")
print("Source Extraxtion Finished: application...")
## Source Extraction -----------------------------------------------------------------------------------------------------------------------------------------------------------

## Data Transformation -----------------------------------------------------------------------------------------------------------------------------------------------------------
#spark.conf.set("spark.sql.session.timeZone", "UTC")
dw_sso_mapping = spark.sql(
"""Select
    map.mapping_id,
    map.app_id,
    app.app_name,
    map.member_id,
    map.account_id,
    map.mapping_token,
    map.data,
    map.created_by,
    map.created_date,
    map.updated_by,
    map.updated_date
    from mapping as map
left join application as app on app.app_id = map.app_id""")

dw_sso_mapping = dw_sso_mapping.withColumn('house', regexp_replace(coalesce(col('data'), lit('')), ';', ''))
dw_sso_mapping = dw_sso_mapping.withColumn('house', split(col("house"), ","))
dw_sso_mapping = dw_sso_mapping.select('mapping_id',
    'app_id',
    'app_name',
    'member_id',
    'account_id',
    'mapping_token',
    'data',
    'created_by',
    'created_date',
    'updated_by',
    'updated_date',
    explode(col('house')).alias('house'))\
    .withColumn('account_id', lit(''))
## Data Transformation -----------------------------------------------------------------------------------------------------------------------------------------------------------

## Loading Result to S3
sso_mapping_output_path = "s3://tpdt-dw/tpdt_dw_sso_mapping/"
dw_sso_mapping.write.format('parquet').mode('overwrite').option("header",True).save(sso_mapping_output_path)






job.commit()