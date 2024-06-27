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
bp_transaction_df = bp_transaction_df.filter('date(created_date) > to_date("2022-12-31")')
bp_transaction_df.createOrReplaceTempView("bp_transaction")
bp_transaction_df.persist()


spending_transaction = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_spending_transaction', transformation_ctx = "spending_transaction")
spending_transaction_df = spending_transaction.toDF()
spending_transaction_df = spending_transaction_df.filter('date(created_date) > to_date("2022-12-31")')
spending_transaction_df.createOrReplaceTempView("spending_transaction")

receipt = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_receipt', transformation_ctx = "receipt")
receipt_df = receipt.toDF()
receipt_df = receipt_df.filter('date(created_date) > to_date("2022-12-31")')
receipt_df.createOrReplaceTempView("receipt")

partner_spending_transaction = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_partner_spending_transaction', transformation_ctx = "partner_spending_transaction")
partner_spending_transaction_df = partner_spending_transaction.toDF()
partner_spending_transaction_df = partner_spending_transaction_df.filter('date(created_date) > to_date("2022-12-31")')
partner_spending_transaction_df.createOrReplaceTempView("partner_spending_transaction")

receipt = spark.sql("""select
concat('M',receipt.mall_id) as LEGACY_Mall__c,
receipt.invoice_no as LEGACY_Invoice_No__c,
cast(receipt.original_amount as string) as LEGACY_Amount__c,
receipt.payment_type_lang1 as LEGACY_Payment_Type__c,
case when receipt_datetime is not null then date_format(receipt.receipt_datetime - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") else date_format(receipt.receipt_date - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") end as LEGACY_Activity_Date_Time__c,
concat('S',receipt.shop_id) as LEGACY_Shop__c,
receipt.shop_name_lang1 as LEGACY_Shop_Name__c,
receipt.status as LEGACY_Status__c,
receipt.updated_by as LEGACY_Receipt_Updated_By__c,
tranx.member_id as LEGACY_Member__c,
tranx.earned_points as LEGACY_Earned_Converted_Redeemed_Points__c,
date_format(tranx.approval_date - INTERVAL 16 hours, "yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Approval_Date__c,
tranx.reject_reason_lang1 as LEGACY_Reject_Reason__c,
tranx.type as LEGACY_Type__c,
case when tranx.auto_approved = 1 then 'TRUE' else 'FALSE' end as LEGACY_Auto_Approved__c,
cast(receipt.ocr_id as string) as LEGACY_OCR_ID__c,
cast(receipt.receipt_id as string) as LEGACY_Receipt_ID__c,
receipt.created_by as LEGACY_Created_By__c,
date_format(receipt.created_date - INTERVAL 16 hours, "yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Created_Date__c,
date_format(receipt.updated_date - INTERVAL 16 hours, "yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Updated_Date__c,
concat('B',partner.partner_id) as LEGACY_Partner_ID__c,
tranx.platform as LEGACY_Platform__c,
tranx.channel as LEGACY_Channel__c,
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
receipt.receipt_image as LEGACY_URL__c,
receipt.electronic_receipt_image as LEGACY_Small_Receipt_URL__c,
bonus.bonus_points_transaction_id as LEGACY_Bonus_point_transaction_ID__c,
bonus.action as LEGACY_Point_Action__c,
'Receipt' as LEGACY_File_Name__c,
'Upload Receipt' as LEGACY_App_Display_Name_EN__c,
'上傳單據' as LEGACY_App_Display_Name_TC__c,
'上传单据' as LEGACY_App_Display_Name_SC__c,
receipt.updated_date as retrieve_timestamp
FROM spending_transaction  tranx
left join receipt receipt
on tranx.spending_transaction_id = receipt.spending_transaction_id
left join partner_spending_transaction partner
on tranx.spending_transaction_id = partner.result
inner join bp_transaction bonus
on tranx.spending_transaction_id = bonus.spending_transaction_id""")


glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(receipt, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_sf_legacy_transaction",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_sf_legacy_transaction"
    )
