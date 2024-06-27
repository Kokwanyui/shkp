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
import pymysql




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


t_bp_transaction = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_bonus_points_transaction',transformation_ctx = "shkpmalls_vip_bp_transaction")
bp_transaction_df = t_bp_transaction.toDF()
bp_transaction_df = bp_transaction_df.filter('date(created_date) > to_date("2022-02-01")')
bp_transaction_df.createOrReplaceTempView("bp_transaction")
bp_transaction_df.persist()

expiry = spark.sql("""SELECT
'' as LEGACY_Mall__c,
'' as LEGACY_Invoice_No__c,
'' as LEGACY_Amount__c,
'' as LEGACY_Payment_Type__c,
date_format(t1.created_date - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'")  as LEGACY_Activity_Date_Time__c,
'' as LEGACY_Shop__c,
'' as LEGACY_Shop_Name__c,
t1.status as LEGACY_Status__c,
'' as LEGACY_Receipt_Updated_By__c,
t1.member_id as LEGACY_Member__c,
t1.points as LEGACY_Earned_Converted_Redeemed_Points__c,
'' as LEGACY_Approval_Date__c,
'' as LEGACY_Reject_Reason__c,
'' as LEGACY_Type__c,
'' as LEGACY_Auto_Approved__c,
'' as LEGACY_OCR_ID__c,
'' as LEGACY_Receipt_ID__c,
'' as LEGACY_Created_By__c,
'' as LEGACY_Created_Date__c,
'' as LEGACY_Updated_Date__c,
'' as LEGACY_Partner_ID__c,
'' as LEGACY_Platform__c,
'' as LEGACY_Channel__c,
'' as LEGACY_Pickup_Point__c,
'' as LEGACY_Reward_Name__c,
'' as LEGACY_Redeemed_Reward_Id__c,
'' as LEGACY_Expiry_Date__c,
'' as LEGACY_Reserve_Date_Time__c,
'' as LEGACY_Cancel_Date_Time__c,
'' as LEGACY_Used_Date_Time__c,
'' as LEGACY_Linked_Sales_Amount__c,
'' as LEGACY_Min_Spending_Amount__c,
'' as LEGACY_Point_Dollar_Transaction_Amount__c,
'' as LEGACY_Payment_Status__c,
'' as LEGACY_URL__c,
'' as LEGACY_Small_Receipt_URL__c,
t1.bonus_points_transaction_id as LEGACY_Bonus_point_transaction_ID__c,
t1.action as LEGACY_Point_Action__c,
'Expiry' as LEGACY_File_Name__c,
'' as LEGACY_App_Display_Name_EN__c,
'' as LEGACY_App_Display_Name_TC__c,
'' as LEGACY_App_Display_Name_SC__c,
t1.updated_date as retrieve_timestamp
FROM bp_transaction t1
where action in ('E','N')
""")


glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(expiry, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_sf_legacy_transaction",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_sf_legacy_transaction"
    )
