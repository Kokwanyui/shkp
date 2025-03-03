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

## udfs
def save_df(df, separator, root_folder):
    # clean all objects in folder
    s3 = boto3.resource('s3')
    clean_bucket = s3.Bucket('tpdt-data-migration-tmp')
    clean_prefix = f'{root_folder}/input/'
    clean_bucket.objects.filter(Prefix=clean_prefix).delete()
    
    # save temp file
    output_path = f"s3://tpdt-data-migration-tmp/{root_folder}/temp"
    df.coalesce(1).write.mode("overwrite").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").option("dateFormat", "yyyy-MM-dd").option("compression", "gzip").csv(output_path, header="true", sep=separator)


def renaming_output(root_folder, tablename):
    # renaming config
    s3 = boto3.client('s3')
    BucketName = 'tpdt-data-migration-tmp'
    FolderName = f'{root_folder}/temp/'
    FinalFolder = f'{root_folder}/'
    
    NewFile = f'{FinalFolder}{tablename}_{suffix}.csv.gz'
    response = s3.list_objects(
        Bucket= BucketName,
        MaxKeys = 2,
        Prefix=FolderName
        )
    OldFile=response['Contents'][0]['Key']
    OldSource=BucketName + '/' +   OldFile
    
    # move file and rename
    rename_file = boto3.resource('s3')
    rename_file.Object(BucketName,NewFile).copy_from(CopySource=OldSource)
    rename_file.Object(BucketName,OldFile).delete()
    
    
    

t_event_log = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = prefix+'carpark_user_event_log', transformation_ctx = "carpark_user_event_log")
event_log_df = t_event_log.toDF().select('lpn',
                                             'member_id',
                                             lit('True').alias('CAR_Inactive__c'),
                                             'type',
                                             date_format(col('event_time'), "yyyy-MM-dd'T'HH:mm:ss'.000z'").alias('CAR_Event_Time__c'),
                                             lit('False').alias('CAR_Validated__c'),
                                             'carpark_id')\
                                             .filter(f"type in (1,2,7) and date(event_time) >= to_date('{date_time}', 'yyyy-MM-dd')")
    
t_carparks = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_carpark_carparks', transformation_ctx = "carparks")
carparks_df = t_carparks.toDF()
    
t_vehicles = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = prefix+'vehicles', transformation_ctx = "vehicles")
today_vehicles_df = t_vehicles.toDF().select('lpn','validationSts',col('memberId').alias('member_id'))
    


event_log_df.createOrReplaceTempView("event")
carparks_df.createOrReplaceTempView("carpark")
today_vehicles_df.createOrReplaceTempView("vehicles")
    
final_data = spark.sql("""
    Select 
    t1.lpn as CAR_Car_Plate__c,
    t1.member_id as MEM_Member_ID__c,
    'FALSE' as CAR_Inactive__c,
    t2.type as CAR_User_Event_Type__c,
    CAR_Event_Time__c,
    case when validationSts = 1 then 'TRUE' else 'FALSE' end as CAR_Validated__c,
    cast(t2.carpark_id as string) as CAR_Car_Park_ID__c,
    concat('M',t3.mallid) as CAR_Mall__c,
    current_date() as retrieve_timestamp
    from vehicles t1
    left join event t2 on upper(trim(t1.lpn)) = upper(trim(t2.lpn)) and t1.member_id = t2.member_id
    left join carpark t3 on t2.carpark_id = t3.id
    
    union distinct 
    
    Select t1.lpn,
    t1.member_id,
    'True' as CAR_Inactive__c,
    t1.type as CAR_User_Event_Type__c,
    CAR_Event_Time__c,
    'FALSE' as CAR_Validated__c,
    cast(t1.carpark_id as string) as CAR_Car_Park_ID__c,
    concat('M',t3.mallid) as CAR_Mall__c,
    current_date() as retrieve_timestamp
    from event t1
    left join vehicles t2 on upper(trim(t2.lpn)) = upper(trim(t1.lpn)) and t1.member_id = t2.member_id
    left join  carpark t3
    on t1.carpark_id = t3.id
    where t2.lpn is null
    """)

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(final_data, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_sf_registered_car",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult/",
    transformation_ctx = "data_migration_public_sf_registered_car"
    )

s3_folder = 'vehicle_log'
sep_by = ','
table_name = 'vehicles'
save_df(today_vehicles_df, sep_by , s3_folder)
renaming_output(s3_folder, table_name)


job.commit()
    


    
    
#ytd_vehicles = spark.read.csv(f's3://tpdt-data-migration-tmp/vehicle_log/vehicles_{yesterday}.csv',header='true', inferSchema='true', sep=',')

