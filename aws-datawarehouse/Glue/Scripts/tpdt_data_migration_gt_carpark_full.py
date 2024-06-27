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


t_carpark_carparks = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_carpark_carparks', transformation_ctx = "shkpmalls_carpark_carparks")
carpark_carparks_df = t_carpark_carparks.toDF()
carpark_carparks_df.createOrReplaceTempView("carpark_carpark")
    
t_cms_parking = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_cms_parking', transformation_ctx = "shkpmalls_cms_parking")
cms_parking_df = t_cms_parking.toDF()
cms_parking_df.createOrReplaceTempView("cms_carpark")

t_vip_mall = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_mall', transformation_ctx = "shkpmalls_vip_mall")
vip_mall_df = t_vip_mall.toDF()
vip_mall_df.createOrReplaceTempView("vip_mall")

t_cms_mall = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_cms_mall', transformation_ctx = "shkpmalls_cms_mall")
cms_mall_df = t_cms_mall.toDF()
cms_mall_df.createOrReplaceTempView("cms_mall")

    
final_data = spark.sql("""
    SELECT
t2.carpark_id as id,
case when t1.parking_name_en is not null then t1.parking_name_en else t2.name end as name_en,
case when t1.parking_name_tc is not null then t1.parking_name_tc else null end as name_tc,
case when t1.parking_name_sc is not null then t1.parking_name_sc else null end as name_sc,
t1.loc_long_en as address_en,
t1.loc_long_tc as address_tc,
t1.loc_long_sc as address_sc,
t1.LOC_LATITUDE as latitude,
t1.LOC_LONGTITUDE as longitude,
t1.get_vacancy_api as parking_lot_vacancy_api,
'' as weekday_parking_offer_desc_en,
'' as weekday_parking_offer_desc_tc,
'' as weekday_parking_offer_desc_sc,
'' as non_weekday_parking_offer_desc_en,
'' as non_weekday_parking_offer_desc_tc,
'' as non_weekday_parking_offer_desc_sc,
'"'"['"'"monday''",'"'"'"tuesday''",'"'"'"wednesday''",'"'"'"thursday''",'"'"'"friday'"'"'"]"' as is_weekday,
current_timestamp() as created_at,
current_timestamp() as updated_at,
current_timestamp() as published_at,
1 as created_by_id,
1 as updated_by_id,
case when t2.carpark_id > 2000 then 'Wilson' else 'Mall' end as car_park_type,
0 as redeemed_by_points,
0 as redeemed_by_spending,
ifnull(t1.ev_charger,0) as ev_charger_status,
t2.mallid as mall_id,
concat(case when t1.parking_name_en is not null then t1.parking_name_en else t2.name end, '_', case when t4.mall_name_en is not null then t4.mall_name_en else t3.name_lang1 end) as alias_key
from (select id as carpark_id, case when (type = 'CL' and cms_carpark_id is not null) then cms_carpark_id
when id = 2001 then 37
when id = 2002 then 94
when id = 2003 then 93
when id = 2004 then 82
when id = 2005 then 121
when id = 2006 then 122
when id = 2007 then 123
when id = 2008 then 83
when id = 2009 then 124
when id = 2010 then 117
when id = 2011 then 115
when id = 2012 then 116
when id = 2013 then 107
when id = 2014 then 75
when id = 2015 then 114
when id = 2016 then 91
when id = 2017 then 90
when id = 2018 then 92
when id = 2019 then 109
when id = 2020 then 108
when id = 2021 then 110
when id = 2022 then 88
when id = 2023 then 84
when id = 2024 then 85
when id = 2025 then 86
when id = 2026 then 87
when id = 2027 then 34
when id = 2028 then 112
when id = 2029 then 96
when id = 2030 then 99
when id = 2031 then 100
when id = 2032 then 97
when id = 2033 then 98
when id = 2034 then 14
when id = 2035 then 89
when id = 2036 then 23
when id = 2037 then 106
when id = 2038 then 105
when id = 2039 then 95
when id = 2040 then 101
when id = 2041 then 102
when id = 2042 then 111
when id = 2043 then 113
when id = 2044 then 104
when id = 2045 then 103
when id = 2046 then 80
when id = 2047 then 81
when id = 2048 then 41
when id = 2049 then 12
when id = 2050 then 11
when id = 2051 then 43
when id = 2052 then 118
when id = 2053 then 119
when id = 2055 then 120
when id = 2025 then 86
when id = 2032 then 97
when id = 2043 then 113
when id = 2054 then 40
end as mapping_id,
type, name, mallid
from carpark_carpark) t2
left join cms_carpark t1
on t2.mapping_id = t1.id
left join vip_mall t3
on t2.mallid = t3.mall_id
left join cms_mall t4
on t3.cms_mall_id = t4.id
    """)

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(final_data, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_car_parks_mall_links",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult/",
    transformation_ctx = "data_migration_public_gt_car_parks_mall_links"
    )
    
job.commit()
    


    
    
#ytd_vehicles = spark.read.csv(f's3://tpdt-data-migration-tmp/vehicle_log/vehicles_{yesterday}.csv',header='true', inferSchema='true', sep=',')

