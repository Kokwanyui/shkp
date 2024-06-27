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
    date_time_60 = (datetime.now() - timedelta(60)).strftime('%Y-%m-%d')
    
prefix="shkpmalls_carpark_"
prefix03='tpdt_'

bp_hashexpression = {"hashexpression": "member_mall_bonus_points_id >= 3607341 and member_mall_bonus_points_id", "Hashpartitions": "10"}
t_bp_transaction = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_bonus_points_transaction', additional_options=bp_hashexpression,transformation_ctx = "shkpmalls_vip_bp_transaction")
bp_transaction_df = t_bp_transaction.toDF()
bp_transaction_df = bp_transaction_df.filter(f'date(updated_date) >= to_date("{date_time}")')
bp_transaction_df.createOrReplaceTempView("bp_transaction")
bp_transaction_df.persist()

bonus_point_to_point_dollar_transaction = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_bonus_point_to_point_dollar_transaction', transformation_ctx = "bonus_point_to_point_dollar_transaction")
bonus_point_to_point_dollar_transaction_df = bonus_point_to_point_dollar_transaction.toDF()
bonus_point_to_point_dollar_transaction_df.createOrReplaceTempView("bonus_point_to_point_dollar_transaction")

point_dollar_transaction = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_point_dollar_transaction', transformation_ctx = "point_dollar_transaction")
point_dollar_transaction_df = point_dollar_transaction.toDF()
point_dollar_transaction_df.createOrReplaceTempView("point_dollar_transaction")

shop = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_shop', transformation_ctx = "shop")
shop_df = shop.toDF()
shop_df.createOrReplaceTempView("shop")

gift_redemption_transaction = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_gift_redemption_transaction', transformation_ctx = "gift_redemption_transaction")
gift_redemption_transaction_df = gift_redemption_transaction.toDF()
gift_redemption_transaction_df = gift_redemption_transaction_df.filter(f'date(updated_date) >= to_date("{date_time_60}")')
gift_redemption_transaction_df.createOrReplaceTempView("gift_redemption_transaction")

gift = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_gift', transformation_ctx = "gift")
gift_df = gift.toDF()
gift_df.createOrReplaceTempView("gift")

payment_transaction = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_payment_transaction', transformation_ctx = "payment_transaction")
payment_transaction_df = payment_transaction.toDF()
payment_transaction_df.createOrReplaceTempView("payment_transaction")

coupon = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_coupon', transformation_ctx = "coupon")
coupon_df = coupon.toDF()
coupon_df.createOrReplaceTempView("coupon")

coupon_redemption_transaction = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_coupon_redemption_transaction', transformation_ctx = "coupon_redemption_transaction")
coupon_redemption_transaction_df = coupon_redemption_transaction.toDF()
coupon_redemption_transaction_df = coupon_redemption_transaction_df.filter(f'date(updated_date) >= to_date("{date_time_60}")')
coupon_redemption_transaction_df.createOrReplaceTempView("coupon_redemption_transaction")

event = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_event', transformation_ctx = "event")
event_df = event.toDF()
event_df.createOrReplaceTempView("event")

event_redemption_transaction = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_event_redemption_transaction', transformation_ctx = "event_redemption_transaction")
event_redemption_transaction_df = event_redemption_transaction.toDF()
event_redemption_transaction_df.createOrReplaceTempView("event_redemption_transaction")



conversion = spark.sql("""SELECT
concat('M',t1.mall_id) as LEGACY_Mall__c,
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
'Conversion' as LEGACY_File_Name__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang1'),'"','') as LEGACY_App_Display_Name_EN__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang2'),'"','') as LEGACY_App_Display_Name_TC__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang3'),'"','') as LEGACY_App_Display_Name_SC__c,
t1.updated_date as retrieve_timestamp
FROM bp_transaction t1
where action = 'D'
and lower(replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang1'),'"','')) like '%conversion%'""")

preaction = "Delete from public.staging_sf_legacy_transaction;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(conversion, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_staging_sf_legacy_transaction",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_sf_legacy_transaction",
    additional_options={"preactions": preaction}
    )


promotion = spark.sql("""SELECT
concat('M',t1.mall_id) as LEGACY_Mall__c,
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
'Promotion' as LEGACY_File_Name__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang1'),'"','') as LEGACY_App_Display_Name_EN__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang2'),'"','') as LEGACY_App_Display_Name_TC__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang3'),'"','') as LEGACY_App_Display_Name_SC__c,
t1.updated_date as retrieve_timestamp
FROM bp_transaction t1
where action = 'D'
and (lower(replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang1'),'"','')) not like '%conversion%'
or lower(replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang1'),'"','')) is null)
""")


glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(promotion, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_staging_sf_legacy_transaction",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_sf_legacy_transaction"
    )
    
adjustment = spark.sql("""Select
distinct
'' as LEGACY_Mall__c,
'' as LEGACY_Invoice_No__c,
'' as LEGACY_Amount__c,
'' as LEGACY_Payment_Type__c,
date_format(t1.created_date - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Activity_Date_Time__c,
'' as LEGACY_Shop__c,
'' as LEGACY_Shop_Name__c,
'' as LEGACY_Status__c,
'' as LEGACY_Receipt_Updated_By__c,
t1.member_id as LEGACY_Member__c,
points as LEGACY_Earned_Converted_Redeemed_Points__c,
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
'Reward/ Point Adjustment' as LEGACY_File_Name__c,
adjust_reason_lang1 as LEGACY_App_Display_Name_EN__c,
adjust_reason_lang2 as LEGACY_App_Display_Name_TC__c,
adjust_reason_lang3 as LEGACY_App_Display_Name_SC__c,
t1.updated_date as retrieve_timestamp
from bp_transaction t1
where action in ('A') and (adjust_reason_lang1 != 'Used Point Dollar Spending' or adjust_reason_lang1 is null)
""")

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(adjustment, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_staging_sf_legacy_transaction",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_sf_legacy_transaction"
    )

point_dollar = spark.sql("""select distinct
concat('M',t3.mall_id) as LEGACY_Mall__c,
'' as LEGACY_Invoice_No__c,
'' as LEGACY_Amount__c,
'' as LEGACY_Payment_Type__c,
date_format(t1.created_date - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Activity_Date_Time__c,
concat('S', t3.shop_id) as LEGACY_Shop__c,
t5.name_lang1 as LEGACY_Shop_Name__c,
t2.status as LEGACY_Status__c,
'' as LEGACY_Receipt_Updated_By__c,
t1.member_id as LEGACY_Member__c,
t1.points as LEGACY_Earned_Converted_Redeemed_Points__c,
'' as LEGACY_Approval_Date__c,
'' as LEGACY_Reject_Reason__c,
t3.trans_type as LEGACY_Type__c,
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
'Point Dollar' as LEGACY_Reward_Name__c,
'' as LEGACY_Redeemed_Reward_Id__c,
'' as LEGACY_Expiry_Date__c,
'' as LEGACY_Reserve_Date_Time__c,
'' as LEGACY_Cancel_Date_Time__c,
'' as LEGACY_Used_Date_Time__c,
'' as LEGACY_Linked_Sales_Amount__c,
'' as LEGACY_Min_Spending_Amount__c,
cast(t3.amount as string) as LEGACY_Point_Dollar_Transaction_Amount__c,
'' as LEGACY_Payment_Status__c,
'' as LEGACY_URL__c,
'' as LEGACY_Small_Receipt_URL__c,
t1.bonus_points_transaction_id as LEGACY_Bonus_point_transaction_ID__c,
t1.action as LEGACY_Point_Action__c,
'Reward' as LEGACY_File_Name__c,
adjust_reason_lang1 as LEGACY_App_Display_Name_EN__c,
adjust_reason_lang2 as LEGACY_App_Display_Name_TC__c,
adjust_reason_lang3 as LEGACY_App_Display_Name_SC__c,
t1.updated_date as retrieve_timestamp
from bp_transaction t1
left join bonus_point_to_point_dollar_transaction t2 on t1.redemption_transaction_id = t2.trans_id
left join point_dollar_transaction t3 on t3.trans_id = t2.point_dollar_transaction_id
left join shop t5 on t3.shop_id = t5.shop_id
where t1.action in ('A') 
and adjust_reason_lang1 = 'Used Point Dollar Spending'
""")

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(point_dollar, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_staging_sf_legacy_transaction",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_sf_legacy_transaction"
    )

free_parking = spark.sql("""Select 
distinct 
'' as LEGACY_Mall__c,
'' as LEGACY_Invoice_No__c,
'' as LEGACY_Amount__c,
'' as LEGACY_Payment_Type__c,
date_format(t1.created_date - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Activity_Date_Time__c,
'' as LEGACY_Shop__c,
'' as LEGACY_Shop_Name__c,
'' as LEGACY_Status__c,
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
'Reward' as LEGACY_File_Name__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang1'),'"','') as LEGACY_App_Display_Name_EN__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang2'),'"','') as LEGACY_App_Display_Name_TC__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang3'),'"','') as LEGACY_App_Display_Name_SC__c,
greatest(t1.updated_date, t2.updated_date) as retrieve_timestamp
from bp_transaction t1
left join gift_redemption_transaction t2 on t1.redemption_transaction_id = t2.gift_redemption_transaction_id
left join gift t3 on t3.gift_id = t2.gift_id
left join payment_transaction t4 on t2.payment_transaction_id = t4.payment_transaction_id
where t1.action in ('C', 'M', 'R', 'T')
and left(t1.redemption_transaction_id,2) = 'GR'
and lower(t3.name_lang1) like '%%free parking%%'""")

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(free_parking, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_staging_sf_legacy_transaction",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_sf_legacy_transaction"
    )

gift = spark.sql("""Select
distinct
'' as LEGACY_Mall__c,
t4.order_reference_no as LEGACY_Invoice_No__c,
cast(t2.redeemed_amount as string) as LEGACY_Amount__c,
t4.payment_type_lang1 as LEGACY_Payment_Type__c,
date_format(t1.created_date - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Activity_Date_Time__c,
'' as LEGACY_Shop__c,
'' as LEGACY_Shop_Name__c,
t2.status as LEGACY_Status__c,
'' as LEGACY_Receipt_Updated_By__c,
t1.member_id as LEGACY_Member__c,
t1.points as LEGACY_Earned_Converted_Redeemed_Points__c,
'' as LEGACY_Approval_Date__c,
'' as LEGACY_Reject_Reason__c,
t2.redeem_type as LEGACY_Type__c,
'' as LEGACY_Auto_Approved__c,
'' as LEGACY_OCR_ID__c,
'' as LEGACY_Receipt_ID__c,
'' as LEGACY_Created_By__c,
date_format(t4.created_date - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Created_Date__c,
'' as LEGACY_Updated_Date__c,
'' as LEGACY_Partner_ID__c,
'' as LEGACY_Platform__c,
'' as LEGACY_Channel__c,
concat('M',t2.mall_id) as LEGACY_Pickup_Point__c,
replace(t3.name_lang1, '"', '') as LEGACY_Reward_Name__c,
t2.gift_id as LEGACY_Redeemed_Reward_Id__c,
'' as LEGACY_Expiry_Date__c,
'' as LEGACY_Reserve_Date_Time__c,
date_format(t2.cancel_datetime - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Cancel_Date_Time__c,
'' as LEGACY_Used_Date_Time__c,
'' as LEGACY_Linked_Sales_Amount__c,
'' as LEGACY_Min_Spending_Amount__c,
'' as LEGACY_Point_Dollar_Transaction_Amount__c,
t4.status as LEGACY_Payment_Status__c,
'' as LEGACY_URL__c,
'' as LEGACY_Small_Receipt_URL__c,
t1.bonus_points_transaction_id as LEGACY_Bonus_point_transaction_ID__c,
t1.action as LEGACY_Point_Action__c,
'Reward' as LEGACY_File_Name__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang1'),'"','') as LEGACY_App_Display_Name_EN__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang2'),'"','') as LEGACY_App_Display_Name_TC__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang3'),'"','') as LEGACY_App_Display_Name_SC__c,
greatest(t1.updated_date, t2.updated_date) as retrieve_timestamp
from bp_transaction t1
left join gift_redemption_transaction t2 on t1.redemption_transaction_id = t2.gift_redemption_transaction_id
left join gift t3 on t3.gift_id = t2.gift_id
left join payment_transaction t4 on t2.payment_transaction_id = t4.payment_transaction_id
where t1.action in ('C', 'M', 'R', 'T') 
and left(t1.redemption_transaction_id,2) = 'GR'
and (lower(t3.name_lang1) not like '%%free parking%%' or t3.name_lang1 is null)""")


glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(gift, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_staging_sf_legacy_transaction",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_sf_legacy_transaction"
    )


coupon = spark.sql("""Select
distinct
concat('M',t2.used_mall_id) as LEGACY_Mall__c,
t4.order_reference_no as LEGACY_Invoice_No__c,
cast(t2.redeemed_amount as string) as LEGACY_Amount__c,
t4.payment_type_lang1 as LEGACY_Payment_Type__c,
date_format(t1.created_date - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Activity_Date_Time__c,
concat ('S', t2.used_shop_id) as LEGACY_Shop__c,
t5.name_lang1 as LEGACY_Shop_Name__c,
t2.status as LEGACY_Status__c,
'' as LEGACY_Receipt_Updated_By__c,
t1.member_id as LEGACY_Member__c,
t1.points as LEGACY_Earned_Converted_Redeemed_Points__c,
'' as LEGACY_Approval_Date__c,
'' as LEGACY_Reject_Reason__c,
t2.redeem_type as LEGACY_Type__c,
'' as LEGACY_Auto_Approved__c,
'' as LEGACY_OCR_ID__c,
'' as LEGACY_Receipt_ID__c,
'' as LEGACY_Created_By__c,
date_format(t4.created_date - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Created_Date__c,
'' as LEGACY_Updated_Date__c,
'' as LEGACY_Partner_ID__c,
'' as LEGACY_Platform__c,
'' as LEGACY_Channel__c,
concat('M',t2.mall_id) as LEGACY_Pickup_Point__c,
replace(t3.name_lang1, '"', '') as LEGACY_Reward_Name__c,
t2.coupon_id as LEGACY_Redeemed_Reward_Id__c,
date_format(t2.expiry_date - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Expiry_Date__c,
'' as LEGACY_Reserve_Date_Time__c,
date_format(t2.cancel_datetime - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Cancel_Date_Time__c,
date_format(t2.used_datetime - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Used_Date_Time__c,
cast(t2.linked_sales_amount as string) as LEGACY_Linked_Sales_Amount__c,
cast(t2.min_spending_amount as string) as LEGACY_Min_Spending_Amount__c,
'' as LEGACY_Point_Dollar_Transaction_Amount__c,
t4.status as LEGACY_Payment_Status__c,
'' as LEGACY_URL__c,
'' as LEGACY_Small_Receipt_URL__c,
t1.bonus_points_transaction_id as LEGACY_Bonus_point_transaction_ID__c,
t1.action as LEGACY_Point_Action__c,
'Reward' as LEGACY_File_Name__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang1'),'"','') as LEGACY_App_Display_Name_EN__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang2'),'"','') as LEGACY_App_Display_Name_TC__c,
replace(get_json_object(replace(replace(get_json_object(matched_rules_json,'$.extra_offer'),'[',''),']',''),'$.name_lang3'),'"','') as LEGACY_App_Display_Name_SC__c,
greatest(t1.updated_date, t2.updated_date) as retrieve_timestamp
from bp_transaction t1
left join coupon_redemption_transaction t2 on t1.redemption_transaction_id = t2.coupon_redemption_transaction_id
left join coupon t3 on t3.coupon_id = t2.coupon_id
left join payment_transaction t4 on t2.payment_transaction_id = t4.payment_transaction_id
left join shop t5 on t2.used_shop_id = t5.shop_id
where t1.action in ('C', 'M', 'R', 'T') 
and left(t1.redemption_transaction_id,2) = 'CR'
""")


glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(coupon, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_staging_sf_legacy_transaction",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_sf_legacy_transaction"
    )

event = spark.sql("""Select
distinct
'' as LEGACY_Mall__c,
'' as LEGACY_Invoice_No__c,
'' as LEGACY_Amount__c,
'' as LEGACY_Payment_Type__c,
date_format(t1.created_date - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Activity_Date_Time__c,
'' as LEGACY_Shop__c,
'' as LEGACY_Shop_Name__c,
t2.status as LEGACY_Status__c,
'' as LEGACY_Receipt_Updated_By__c,
t1.member_id as LEGACY_Member__c,
t1.points as LEGACY_Earned_Converted_Redeemed_Points__c,
'' as LEGACY_Approval_Date__c,
'' as LEGACY_Reject_Reason__c,
t2.redeem_type as LEGACY_Type__c,
'' as LEGACY_Auto_Approved__c,
'' as LEGACY_OCR_ID__c,
'' as LEGACY_Receipt_ID__c,
'' as LEGACY_Created_By__c,
'' as LEGACY_Created_Date__c,
'' as LEGACY_Updated_Date__c,
'' as LEGACY_Partner_ID__c,
'' as LEGACY_Platform__c,
'' as LEGACY_Channel__c,
concat('M',t2.mall_id) as LEGACY_Pickup_Point__c,
replace(t2.event_name_lang1,'"', '') as LEGACY_Reward_Name__c,
cast(t2.event_id as string) as LEGACY_Redeemed_Reward_Id__c,
'' as LEGACY_Expiry_Date__c,
date_format(t2.event_session_end_time - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Reserve_Date_Time__c,
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
'Reward' as LEGACY_File_Name__c,
replace(t3.name_lang1,'"', '') as LEGACY_App_Display_Name_EN__c,
replace(t3.name_lang2,'"', '') as LEGACY_App_Display_Name_TC__c,
replace(t3.name_lang3,'"', '') as LEGACY_App_Display_Name_SC__c,
greatest(t1.updated_date, t2.updated_date) as retrieve_timestamp
from bp_transaction t1
left join event_redemption_transaction t2 on t1.redemption_transaction_id = t2.event_redemption_transaction_id
left join event t3 on t3.event_id = t2.event_id
where t1.action in ('C', 'M', 'R', 'T') 
and left(t1.redemption_transaction_id,2) = 'ER'
""")


glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(event, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_staging_sf_legacy_transaction",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_sf_legacy_transaction"
    )


reward = spark.sql("""Select
distinct
'' as LEGACY_Mall__c,
'' as LEGACY_Invoice_No__c,
'' as LEGACY_Amount__c,
'' as LEGACY_Payment_Type__c,
date_format(t1.created_date - INTERVAL 16 hours,"yyyy-MM-dd'T'HH:mm:ss'.000Z'") as LEGACY_Activity_Date_Time__c,
'' as LEGACY_Shop__c,
'' as LEGACY_Shop_Name__c,
'' as LEGACY_Status__c,
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
'Reward' as LEGACY_File_Name__c,
'' as LEGACY_App_Display_Name_EN__c,
'' as LEGACY_App_Display_Name_TC__c,
'' as LEGACY_App_Display_Name_SC__c,
t1.updated_date as retrieve_timestamp
from bp_transaction t1
where t1.action in ('C', 'M', 'R', 'T') 
and t1.redemption_transaction_id is null""")


glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(reward, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_staging_sf_legacy_transaction",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_sf_legacy_transaction"
    )

spending_transactio_query = """Select * from shkpmalls_vip.spending_transaction where date(updated_date)>= '2023-06-01'"""
spending_transaction = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_spending_transaction', transformation_ctx = "spending_transaction",additional_options={'preactions': spending_transactio_query})
spending_transaction_df = spending_transaction.toDF()
spending_transaction_df = spending_transaction_df.filter(f'date(updated_date) >= to_date("{date_time_60}")')
spending_transaction_df.createOrReplaceTempView("spending_transaction")

receipt_query = """Select * from shkpmalls_vip.receipt where receipt_id >= 61882404"""
receipt = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_receipt', transformation_ctx = "receipt",
    additional_options={'preactions': receipt_query})
receipt_df = receipt.toDF()
receipt_df = receipt_df.filter(f'date(updated_date) >= to_date("{date_time_60}")')
receipt_df.createOrReplaceTempView("receipt")

partner_spending_transaction = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_partner_spending_transaction', transformation_ctx = "partner_spending_transaction")
partner_spending_transaction_df = partner_spending_transaction.toDF()
partner_spending_transaction_df = partner_spending_transaction_df.filter(f'date(updated_date) >= to_date("{date_time_60}")')
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
    table_name = "data_migration_public_staging_sf_legacy_transaction",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_sf_legacy_transaction"
    )
    
upsert = spark.sql("""Select
'' as LEGACY_Mall__c,
'' as LEGACY_Invoice_No__c,
'' as LEGACY_Amount__c,
'' as LEGACY_Payment_Type__c,
'' as LEGACY_Activity_Date_Time__c,
'' as LEGACY_Shop__c,
'' as LEGACY_Shop_Name__c,
'' as LEGACY_Status__c,
'' as LEGACY_Receipt_Updated_By__c,
'' as LEGACY_Member__c,
0 as LEGACY_Earned_Converted_Redeemed_Points__c,
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
'' as LEGACY_Bonus_point_transaction_ID__c,
'' as LEGACY_Point_Action__c,
'' as LEGACY_File_Name__c,
'' as LEGACY_App_Display_Name_EN__c,
'' as LEGACY_App_Display_Name_TC__c,
'' as LEGACY_App_Display_Name_SC__c,
current_timestamp() as retrieve_timestamp
""")

pre_action = "call public.update_sf_legacy_transaction();"
glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(upsert, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_staging_sf_legacy_transaction",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_sf_legacy_transaction",
    additional_options={"preactions": pre_action}
    )