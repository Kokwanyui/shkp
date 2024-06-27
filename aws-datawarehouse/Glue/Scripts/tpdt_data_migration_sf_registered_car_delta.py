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
from pyspark.sql.window import Window




args = getResolvedOptions(sys.argv, ['JOB_NAME', 'env', 'loading'])
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
loading = args['loading']
if env == 'prod':
    db01="tpdt_01replica"
    db02="tpdt_02replica"
    db03="tpdt_03dw"
    db03sub="tpdt_03sub"
    output_path="s3://tpdt-adhoc/bp_transaction_json/"
    staging_path="s3://tpdt-staging/"
    dimension_path="s3://tpdt-dimension/"
    district_mapping_s3_location = 's3://tpdt-dimension/tpdt_dim_tp_district_mapping/tpdt_dim_tp_district_mapping.csv'

else:
    db01="tpdt_01replica_"+env
    db03="tpdt_03dw_"+env
    db03sub="tpdt_03sub_"+env
    output_path="s3://tpdt-dw-"+env+"/"+table_name+"/"
    staging_path="s3://tpdt-staging-"+env+"/"
    dimension_path="s3://tpdt-dimension-"+env+"/"
    district_mapping_s3_location = 's3://tpdt-dimension-'+env+'/tpdt_dim_tp_district_mapping/tpdt_dim_tp_district_mapping.csv'

yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')
if loading == 'full':
    date_time = str('2000-01-01')
else:
    date_time = (datetime.now() - timedelta(3)).strftime('%Y-%m-%d')
    
prefix="shkpmalls_carpark_"
prefix03='tpdt_'

t_vehicles = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_carpark_vehicles', transformation_ctx = "shkpmalls_carpark_vehicles")
vehicles_df = t_vehicles.toDF()
vehicles_df.createOrReplaceTempView("vehicles")
    
t_carpark_user_event_log = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_carpark_carpark_user_event_log', transformation_ctx = "shkpmalls_carpark_carpark_user_event_log")
carpark_user_event_log_df = t_carpark_user_event_log.toDF()
carpark_user_event_log_df.createOrReplaceTempView("carpark_user_event_log")

t_shkpmalls_carpark_carparks = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_carpark_carparks', transformation_ctx = "shkpmalls_carpark_carparks")
shkpmalls_carpark_carparks_df = t_shkpmalls_carpark_carparks.toDF()
shkpmalls_carpark_carparks_df.createOrReplaceTempView("carparks")

sf_registered_car_df = spark.sql("""
Select vehicles.lpn as CAR_Car_Plate__c,
memberId as MEM_Member_ID__c,
'FASLE' as CAR_Inactive__c,
type as CAR_User_Event_Type__c,
event_time as CAR_Event_Time__c,
case when validationSts = 1 then 'TRUE' else 'FALSE' end as CAR_Validated__c,
cast(carpark_id as string) as CAR_Car_Park_ID__c,
concat('M',mallid) as CAR_Mall__c
FROM vehicles vehicles
left join carpark_user_event_log log
on upper(trim(vehicles.lpn)) = upper(trim(log.lpn)) and log.member_id = vehicles.memberId
left join (select id, mallid FROM carparks) carpark
on log.carpark_id = carpark.id
where type in (1,2,7)

union all

SELECT
log.lpn as CAR_Car_Plate__c,
member_id as MEM_Member_ID__c,
'TRUE' as CAR_Inactive__c,
type as CAR_User_Event_Type__c,
event_time as CAR_Event_Time__c,
'FALSE' as CAR_Validated__c,
cast(carpark_id as string) as CAR_Car_Park_ID__c,
concat('M',mallid) as CAR_Mall__c
FROM carpark_user_event_log log
left join vehicles vehicles
on upper(trim(vehicles.lpn)) = upper(trim(log.lpn)) and log.member_id = vehicles.memberId
left join (select id, mallid FROM carparks) carpark
on log.carpark_id = carpark.id
where type in (1,2,7)
and vehicles.lpn is null
""")

preaction = "Delete from public.sf_registered_car;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(sf_registered_car_df, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_sf_registered_car",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_sf_registered_car",
    additional_options={"preactions": preaction}
    )


    