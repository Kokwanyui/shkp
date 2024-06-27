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

t_vip_member = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_member', transformation_ctx = "shkpmalls_vip_member")
vip_member_df = t_vip_member.toDF()
vip_member_df.createOrReplaceTempView("vip_member")
    
t_member_setting = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_member_setting', transformation_ctx = "shkpmalls_vip_member_setting")
member_setting_df = t_member_setting.toDF()
member_setting_df.createOrReplaceTempView("member_setting")

t_partner_member_mapping = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_partner_member_mapping', transformation_ctx = "shkpmalls_vip_partner_member_mapping")
partner_member_mapping_df = t_partner_member_mapping.toDF()
partner_member_mapping_df.createOrReplaceTempView("partner_member_mapping")

partner_binding_raw = spark.sql("""Select 
                                    Distinct t2.member_id,
                                    t1.app_id, 
                                    t1.status, 
                                    t1.updated_date 
                                    from partner_member_mapping t1
                                    left join vip_member t2 on t1.phone = t2.phone
                                    where date(t1.updated_date) >= date_sub(current_date(), 1)
                                    """)

cols = ["app_id", "member_id"]
window = Window.partitionBy(cols).orderBy(col('updated_date').desc())
partner_binding_raw = partner_binding_raw.withColumn('last_action', row_number().over(window))

yata_binding = partner_binding_raw.filter("last_action = 1 and app_id = 5001")
smt_binding = partner_binding_raw.filter("last_action = 1 and app_id = 5004")
goroyal_binding = partner_binding_raw.filter("last_action = 1 and app_id = 6001")


yata_binding_preaction = "Delete from public.staging_yata_binding;"
smt_binding_preaction = "Delete from public.staging_smartone_binding;"
goroyal_binding_preaction = "Delete from public.staging_goroyal_binding;"


    


sf_member_df = spark.sql("""
Select '' as email,
12345678 as MobilePhone,
title as Salutation,
first_name_lang1 as FirstName,
last_name_lang1 as LastName,
concat('M',from_mall_id) as MEM_Registration_Source__c,
from_mall_name as MEM_From_Mall_Name__c,
t1.member_id as MEM_Member_ID__c,
age as MEM_Age_CMS__c,
birth_mth as MEM_Month_of_Birthday__c,
first_name_lang2 as MEM_First_Name_TC__c,
last_name_lang2 as MEM_Last_Name_TC__c,
phone_prefix as MEM_Country_Code__c,
date_format(registration_date, "yyyy-MM-dd'T'HH:mm:ss")as MEM_Member_Creation_Date__c,
t1.status as MEM_Member_Status__c,
country_lang1 as MailingCountry,
city_lang1 as MailingCity,
case
when district_lang1 = 'N/A' and sub_district_lang1 = 'N/A' then null
when district_lang1 != 'N/A' and sub_district_lang1 = 'N/A' then district_lang1
when district_lang1 = 'N/A' and sub_district_lang1 != 'N/A' then sub_district_lang1
when district_lang1 != 'N/A' and sub_district_lang1 != 'N/A' then concat(district_lang1,' , ',sub_district_lang1)
when district_lang1 = 'Undefined' and sub_district_lang1 = 'Undefined' then 'Undefined'
end as MailingStreet
from vip_member t1
where date(update_date) >= date_sub(current_date(), 1)
""")

preaction = "Delete from public.staging_sf_member;"
postaction = "call public.update_sf_member_stagin()"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(sf_member_df, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_sf_member",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_sf_member",
    additional_options={"preactions": preaction, "preactions":postaction}
    )


member_setting = spark.sql("""
select distinct member_id,
default_language as MEM_Default_Language__c,
case when receive_promotion_call = 1 then 'TRUE' else 'FALSE' end as MEM_Receive_Promotion_Call__c,
case when receive_promotion_email = 1 then 'TRUE' else 'FALSE' end as MEM_Receive_Promotion_Email__c,
case when receive_promotion_sms = 1 then 'TRUE' else 'FALSE' end as MEM_Receive_Promotion_SMS__c,
case when receive_promotion_mail = 1 then 'TRUE' else 'FALSE' end as MEM_Receive_Promotion_Mail__c,
case when receive_promotion_msg = 1 then 'TRUE' else 'FALSE' end as MEM_Receive_Promotion_Msg__c
from member_setting
where date(updated_date) >= date_sub(current_date(), 1)
""")

member_setting_preaction = "Delete from public.staging_member_setting;"


yata_binding_postaction = """
Begin;
update public.sf_member t1
set mem_yata_member__c = t2.member_id,
mem_yata_member_bind_date__c = t2.updated_date,
retrival_date = current_date
from public.staging_yata_binding t2
where mem_member_id__c = t2.member_id;
End;
"""

smt_binding_postaction = """
Begin;
update public.sf_member t1
set mem_smartone_member__c = t2.member_id,
mem_smartone_member_bind_date__c = t2.updated_date,
retrival_date = current_date
from public.staging_smartone_binding t2
where mem_member_id__c = t2.member_id;
End;
"""

goroyal_binding_postaction = """
Begin;
update public.sf_member t1
set mem_goroyal_member__c = t2.member_id,
mem_goroyal_member_bind_date__c = t2.updated_date,
retrival_date = current_date
from public.staging_goroyal_binding t2
where mem_member_id__c = t2.member_id;
End;
"""

member_setting_postaction = """
Begin;
update public.sf_member t1
set MEM_Default_Language__c = t2.MEM_Default_Language__c,
MEM_Receive_Promotion_Call__c = t2.MEM_Receive_Promotion_Call__c ,
MEM_Receive_Promotion_Email__c = t2.MEM_Receive_Promotion_Email__c,
MEM_Receive_Promotion_SMS__c = t2.MEM_Receive_Promotion_SMS__c,
MEM_Receive_Promotion_Mail__c = t2.MEM_Receive_Promotion_Mail__c,
MEM_Receive_Promotion_Msg__c = t2.MEM_Receive_Promotion_Msg__c,
retrival_date = current_date
from public.staging_member_setting t2
where mem_member_id__c = t2.member_id;
End;
"""


glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(yata_binding, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_staging_yata_binding",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_yata_binding",
    additional_options={"preactions": yata_binding_preaction, "preactions":yata_binding_postaction}
    )

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(smt_binding, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_staging_smartone_binding",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_smartone_binding",
    additional_options={"preactions": smt_binding_preaction, "preactions":smt_binding_postaction}
    )

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(goroyal_binding, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_staging_goroyal_binding",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_goroyal_binding",
    additional_options={"preactions": goroyal_binding_preaction, "preactions":goroyal_binding_postaction}
    )
    
glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(member_setting, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_staging_member_setting",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_staging_member_setting",
    additional_options={"preactions": member_setting_preaction, "preactions":member_setting_postaction}
    )