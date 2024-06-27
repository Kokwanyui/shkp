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

t_carpark_carparks = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_carpark_carparks', transformation_ctx = "shkpmalls_carpark_carparks")
carpark_carparks_df = t_carpark_carparks.toDF()
carpark_carparks_df.createOrReplaceTempView("carpark_carpark")
    
t_cms_parking = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_cms_parking', transformation_ctx = "shkpmalls_cms_parking")
cms_parking_df = t_cms_parking.toDF()
cms_parking_df.createOrReplaceTempView("cms_carpark")

t_cms_mall = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_cms_mall', transformation_ctx = "shkpmalls_cms_mall")
cms_mall_df = t_cms_mall.toDF()
cms_mall_df.createOrReplaceTempView("cms_mall")
    
t_vip_mall = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_mall', transformation_ctx = "shkpmalls_vip_mall")
vip_mall_df = t_vip_mall.toDF()
vip_mall_df.createOrReplaceTempView("vip_mall")

t_cms_phase = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_cms_phase', transformation_ctx = "shkpmalls_cms_phase")
cms_phase_df = t_cms_phase.toDF()
cms_phase_df.createOrReplaceTempView("cms_phase")

t_trade_cat = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_brand_category', transformation_ctx = "shkpmalls_vip_brand_category")
trade_cat_df = t_trade_cat.toDF()
trade_cat_df.createOrReplaceTempView("trade_cat")

t_floor_plan = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_cms_floor_plan', transformation_ctx = "shkpmalls_cms_floor_plan")
floor_plan_df = t_floor_plan.toDF()
floor_plan_df.createOrReplaceTempView("floor_plan")

t_phase = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_cms_phase', transformation_ctx = "shkpmalls_cms_phase")
phase_df = t_phase.toDF()
phase_df.createOrReplaceTempView("phase")

t_shop_cat = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_cms_shop_cat', transformation_ctx = "shkpmalls_cms_shop_cat")
shop_cat_df = t_shop_cat.toDF()
shop_cat_df.createOrReplaceTempView("shop_cat")

t_brand = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_brand', transformation_ctx = "shkpmalls_vip_brand")
brand_df = t_brand.toDF()
brand_df.createOrReplaceTempView("brand")

t_partner = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_partner', transformation_ctx = "shkpmalls_vip_partner")
partner_df = t_partner.toDF()
partner_df.createOrReplaceTempView("partner")

t_brand_category = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_brand_category', transformation_ctx = "shkpmalls_vip_brand_category")
brand_category_df = t_brand_category.toDF()
brand_category_df.createOrReplaceTempView("brand_category")


t_vip_shop = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_shop', transformation_ctx = "shkpmalls_vip_shop")
vip_shop_df = t_vip_shop.toDF()
vip_shop_df.createOrReplaceTempView("vip_shop")

t_cms_shop = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_cms_shop', transformation_ctx = "shkpmalls_cms_shop")
cms_shop_df = t_cms_shop.toDF()
cms_shop_df.createOrReplaceTempView("cms_shop")

t_partner_shop = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_partner_shop', transformation_ctx = "shkpmalls_vip_partner_shop")
vip_partner_shop_df = t_partner_shop.toDF()
vip_partner_shop_df.createOrReplaceTempView("vip_partner_shop")

t_member = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = 'shkpmalls_vip_member', transformation_ctx = "shkpmalls_vip_member")
member_df = t_member.toDF()
member_df.createOrReplaceTempView("member")

t_carpark_redemption_mapping = glueContext.create_dynamic_frame.from_catalog(database = 'tpdt_03redshift', table_name = 'data_migration_public_gt_carpark_redemption_mapping', redshift_tmp_dir = "s3://tpdt-athena-queryresult",transformation_ct="gt_carpark_redemption_mapping")
carpark_redemption_mapping_df = t_carpark_redemption_mapping.toDF()
carpark_redemption_mapping_df.createOrReplaceTempView("carpark_redemption_mapping")

# mall
mall = spark.sql("""
    SELECT
        case when t2.mall_name_en is not null then t2.mall_name_en else t1.name_lang1 end as name_en,
        case when t2.mall_name_tc is not null then t2.mall_name_tc else t1.name_lang2 end as name_tc,
        case when t2.mall_name_sc is not null then t2.mall_name_sc else t1.name_lang3 end as name_sc,
        case when t2.DESCRIPTION_ENG is not null then t2.DESCRIPTION_ENG else '' end as desc_en,
        case when t2.DESCRIPTION_TCH is not null then t2.DESCRIPTION_TCH else '' end as desc_tc,
        case when t2.DESCRIPTION_SCH is not null then t2.DESCRIPTION_SCH else '' end as desc_sc,
        case when t2.LOC_LONG_EN is not null then t2.LOC_LONG_EN else '' end as address_en,
        case when t2.LOC_LONG_TC is not null then t2.LOC_LONG_TC else '' end  as address_tc,
        case when t2.LOC_LONG_SC is not null then t2.LOC_LONG_SC else '' end  as address_sc,
        case when t2.DISTRICT is null then ''
        when t2.DISTRICT = 0 then 'HONG_KONG_ISLAND'
        when t2.DISTRICT = 1 then 'KOWLOON'
        else 'NEW_TERRITORIES' end as district,
        case when loc_short_en is not null then loc_short_en else '' end as location_en,
        case when loc_short_tc is not null then loc_short_tc else '' end as location_tc,
        case when loc_short_sc is not null then loc_short_sc else '' end as location_sc,
        case when t2.open_hour_en is not null then t2.open_hour_en else '' end as opening_hour_en,
        case when t2.open_hour_tc is not null then t2.open_hour_tc else '' end as opening_hour_tc,
        case when t2.open_hour_sc is not null then t2.open_hour_sc else '' end as opening_hour_sc,
        t1.phone,
        case when t2.LOC_LATITUDE is not null then t2.LOC_LATITUDE else 0 end as latitude,
        case when t2.LOC_LONGITUDE is not null then t2.LOC_LONGITUDE else 0 end as longitude,
        case when left(t2.WEBSITE_URL,3) = 'www' then concat('http://', t2.WEBSITE_URL) else t2.WEBSITE_URL end as WEBSITE_URL,
        case when t2.email is not null then t2.email else '' end as email,
        case when services_webview_eng is not null then concat('https://shkpmalls.shkp.com',  services_webview_eng) else null end as service_website_url_en,
        case when services_webview_tch is not null then concat('https://shkpmalls.shkp.com',  services_webview_tch) else null end as service_website_url_tc,
        case when services_webview_sch is not null then concat('https://shkpmalls.shkp.com',  services_webview_sch) else null end as service_website_url_sc,
        case when left(facebook_url,3) = 'www' then concat('http://', facebook_url) else facebook_url end as facebook_url,
        instagram_url,
        weibo_url,
        case when t1.status = 'A' then 1 else 0 end as under_loyalty_program,
        t1.created_date as created_at,
        t1.updated_date as updated_at,
        now() as published_at,
        1 as created_by_id,
        1 as updated_by_id,
        case when t1.mall_id = 55 then 45 else t1.mall_id end as mall_id,
        case when t1.mall_id in (54,55) then 'ACTIVE'
        when t1.status = 'A' then 'ACTIVE' 
        when t1.status = 'I' then 'INACTIVE'
        when t1.status = 'V' then 'INVALID'
        else t1.status end as status,
        case when t1.mall_id between 2000 and 2100 then 'SMART_PARKING'
        when t1.mall_id between 6000 and 6100 then 'GO_ROYAL'
        when t1.mall_id between 5000 and 5100 then 'PARTNER'
        when t1.mall_id >= 7000 then 'PARTNER'
        when t1.mall_id < 900 then 'THE_POINT'
        else 'OTHER' end as mall_type,
        ifnull(t2.status,0) as show_in_api
        FROM vip_mall t1
        left join cms_mall t2 on (case when mall_id = 54 then 54 when mall_id = 55 then 45 else t1.cms_mall_id end) = t2.id
        left join (select mall, ticket_api from cms_phase where left(ticket_api,4) = 'http' and phase_no = 1) t3 on t1.mall_id = t3.mall
        left join (select mall, maxim_api from cms_phase where left(maxim_api,4) = 'http' and phase_no = 1) t4 on t1.mall_id = t4.mall
        where t1.mall_id not in (1002)
    """)

mall_preaction = "Delete from data_migration.public.gt_mall;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(mall, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_mall",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_mall",
    additional_options={"preactions": mall_preaction}
    )

# brand cat
trade_cat = spark.sql("""SELECT
brand_category_id as id,
created_date as created_at,
updated_date as updated_at,
current_timestamp() as published_at,
1 as created_by_id,
1 as updated_by_id,
name_lang1 as brand_cat_name_en,
name_lang2 as brand_cat_name_tc,
name_lang3 as brand_cat_name_sc
FROM trade_cat
where status = 'A';""")

trade_cat_preaction = "Delete from public.gt_brand_categories;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(trade_cat, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_brand_categories",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "gt_brand_categories",
    additional_options={"preactions": trade_cat_preaction}
    )

# carpark
carpark = spark.sql("""SELECT
t2.carpark_id as id,
case when t1.parking_name_en is not null then t1.parking_name_en else t2.name end as name_en,
case when t1.parking_name_tc is not null then t1.parking_name_tc else '' end as name_tc,
case when t1.parking_name_sc is not null then t1.parking_name_sc else '' end as name_sc,
case when t1.loc_long_en is not null then t1.loc_long_en else '' end as address_en,
case when t1.loc_long_tc is not null then t1.loc_long_tc else '' end as address_tc,
case when t1.loc_long_sc is not null then t1.loc_long_sc else '' end as address_sc,
case when t1.LOC_LATITUDE is not null then t1.LOC_LATITUDE else 0 end as latitude,
case when t1.LOC_LONGTITUDE is not null then t1.LOC_LONGTITUDE else 0 end as longitude,
case when t1.get_vacancy_api is not null then t1.get_vacancy_api else '' end as parking_lot_vacancy_api,
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
case when t5.point is not null then t5.point else 0 end as redeemed_by_points,
case when t5.spending is not null then t5.spending else 0 end as redeemed_by_spending,
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
left join carpark_redemption_mapping t5
on t5.id = t2.carpark_id""")

car_parks_preaction = "Delete from public.gt_car_parks;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(carpark, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_car_parks",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_car_parks",
    additional_options={"preactions": car_parks_preaction}
    )

carpark_mall_link = spark.sql("""
Select id as car_park_id,
mallid as mall_id
from carpark_carpark
""")

car_parks_mall_links_preaction = "Delete from data_migration.public.gt_car_parks_mall_links;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(carpark_mall_link, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_car_parks_mall_links",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_car_parks",
    additional_options={"preactions": car_parks_mall_links_preaction}
    )
    
# floor plan

floorplan = spark.sql("""Select
Distinct
t0.id,
ifnull(create_date,last_update_date) as created_at,
ifnull(last_update_date, create_date) as updated_at,
current_timestamp() as published_at,
1 as  created_by_id,
1 as updated_by_id,
floor_name_en as floor_name_en,
floor_name_tc as floor_name_tc,
floor_name_sc as floor_name_sc,
MALL as mall_id
from floor_plan t0
where t0.status in (1) or t0.id in (74, 146, 171, 219, 220, 252, 253 , 260)
""")
floor_plan_preaction = "Delete from data_migration.public.gt_floor_plan;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(floorplan, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_floor_plan",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_floor_plan",
    additional_options={"preactions": floor_plan_preaction}
    )
    

floor_plans_phase_link = spark.sql("""select id as floor_plan_id,floor_phase as phase_id from floor_plan where status = 1 or id in (74, 146, 171, 219, 220, 252, 253 , 260)""")
window2 = Window.partitionBy("phase_id").orderBy("floor_plan_id")
floor_plans_phase_link = floor_plans_phase_link.withColumn('floor_plan_order', rank().over(window2))
floor_plans_phase_link_preaction = "Delete from public.gt_floor_plans_phase_links;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(floor_plans_phase_link, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_floor_plans_phase_links",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_floor_plans_phase_links",
    additional_options={"preactions": floor_plans_phase_link_preaction}
    )

# phase

phase = spark.sql("""Select * from (
SELECT
t1.PHASE_ID as id,
case when t1.phone != '' then t1.phone
when t1.phone = '' and t1.PHASE_ID in (75,76,83) then '2679 9300' 
else null end as phone,
current_timestamp() as created_at,
current_timestamp() as updated_at,
current_timestamp() as published_at,
1 as created_by_id,
1 as updated_by_id,
case when t1.phase_name_eng = '' then (case when t4.mall_name_en is not null then t4.mall_name_en else t3.name_lang1 end) else t1.phase_name_eng end as phase_name_en,
case when t1.phase_name_tch = '' then (case when t4.mall_name_tc is not null then t4.mall_name_tc else t3.name_lang2 end) else t1.phase_name_tch end as phase_name_tc,
case when t1.PHASE_NAME_SCH = '' then (case when t4.mall_name_sc is not null then t4.mall_name_sc else t3.name_lang3 end) else t1.phase_name_sch end  as phase_name_sc,
case when t3.mall_id is null then t4.id else t3.mall_id end as mall_id,
concat(case when t1.phase_name_eng = '' then (case when t4.mall_name_en is not null then t4.mall_name_en else t3.name_lang1 end) else t1.phase_name_eng end, '_', case when t4.mall_name_en is not null then t4.mall_name_en else t3.name_lang1 end) as alias_key,
t1.TICKET_API as eat_easy_mall_domain,
t1.MAXIM_API as maxim_mall_domain
FROM cms_phase t1
left join cms_mall t4
on t1.mall = t4.id
left join vip_mall t3
on t1.mall = t3.cms_mall_id
where t1.status = 1 and (t3.mall_id is not null or t1.mall in (45,54))  )x
where mall_id != 1002;
""")
phase_preaction = "Delete from public.gt_phases;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(phase, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_phases",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_phases",
    additional_options={"preactions": phase_preaction}
    )

default_floor = spark.sql("""SELECT distinct t1.phase_id, cast(t1.default_floor as int) as floor_plan_id
FROM phase t1
inner join floor_plan t2 on t1.default_floor  = t2.id
where t1.status = 1""")
default_floor_preaction = "Delete from public.gt_phases_default_floor_link;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(default_floor, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_phases_default_floor_link",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_phases_default_floor_link",
    additional_options={"preactions": default_floor_preaction}
    )

phase_mall_link = spark.sql("""Select distinct phase_id, mall as mall_id from phase p left join vip_mall t1 on p.mall = t1.cms_mall_id
where p.status = 1 and (t1.mall_id is not null or p.mall in (45,54)) """)
phases_mall_links_preaction = "Delete from public.gt_phases_mall_links;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(phase_mall_link, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_phases_mall_links",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_phases_mall_links",
    additional_options={"preactions": phases_mall_links_preaction}
    )   

main_shop_cat = spark.sql("""select
id,
category_name_en as shop_main_cat_name_en,
category_name_tc as shop_main_cat_name_tc,
category_name_sc as shop_main_cat_name_sc,
case when type = 1 then 'Dining' else 'Shopping' end as shop_type,
current_timestamp() as created_at,
current_timestamp() as updated_at,
current_timestamp() as published_at,
1 as created_by_id,
1 as updated_by_id
FROM shop_cat
where main_cat is null
and status = 1""")
shop_main_categories_preaction = "Delete from data_migration.public.gt_shop_main_categories;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(main_shop_cat, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_shop_main_categories",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_shop_main_categories",
    additional_options={"preactions": shop_main_categories_preaction}
    )   
    
shop_sub_cat = spark.sql("""select
t1.id as id,
current_timestamp() as created_at,
current_timestamp() as updated_at,
current_timestamp() as published_at,
1 as created_by_id,
1 as updated_by_id,
t1.category_name_en as shop_sub_cat_name_en,
t1.category_name_tc as shop_sub_cat_name_tc,
t1.category_name_sc as shop_sub_cat_name_sc,
concat(t1.category_name_en, '_', t2.category_name_en) as alias_key
FROM shop_cat t1
left join (Select Distinct id, category_name_en from shop_cat where main_cat is null and status = 1) t2 on t1.main_cat = t2.id
where t1.id not in (select distinct id from shop_cat where main_cat is null and status = 1)
and status = 1""")
shop_sub_categories_preaction = "Delete from data_migration.public.gt_shop_sub_categories;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(shop_sub_cat, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_shop_sub_categories",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_shop_sub_categories",
    additional_options={"preactions": shop_sub_categories_preaction}
    )   
    
shop_main_sub_cat_link = spark.sql("""select
id as shop_sub_category_id,
main_cat as shop_main_category_id
FROM shop_cat c
where id not in (select distinct id from shop_cat where main_cat is null and status = 1)
and status = 1""")


shop_sub_categories_shop_main_cat_links_preaction = "Delete from public.gt_shop_sub_categories_shop_main_cat_links;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(shop_main_sub_cat_link, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_shop_sub_categories_shop_main_cat_links",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_shop_sub_categories_shop_main_cat_links",
    additional_options={"preactions": shop_sub_categories_shop_main_cat_links_preaction}
    )   


# brand
brand = spark.sql("""SELECT
Distinct
brand_id as id,
t1.name_lang1 as name_en,
t1.name_lang2 as name_tc,
t1.name_lang3 as name_sc,
0 as is_partner,
t1.created_date as created_at,
t1.updated_date as updated_at,
current_timestamp()  as published_at,
1 as created_by_id,
1 as updated_by_id,
null as partner_account_binding_link,
description_lang1 as partner_desc_en,
description_lang2 as partner_desc_tc,
description_lang3 as partner_desc_sc,
0 as have_year_round_offer,
0 as partner_sequence_number
FROM brand t1
left join brand_category t2 on t1.brand_category_id = t2.brand_category_id
where t1.status = 'A' and brand_id not in (2,19,189)

union all

SELECT
Distinct
partner_id as id,
t1.name_lang1 as name_en,
t1.name_lang2 as name_tc,
t1.name_lang3 as name_sc,
1 as is_partner,
ifnull(t1.created_date,current_timestamp())  as created_at,
ifnull(t1.updated_date,current_timestamp()) as updated_at,
current_timestamp()  as published_at,
1 as created_by_id,
1 as updated_by_id,
case when partner_id = 5001 then 'https://yatahk.page.link/homeYata'
when  partner_id = 5004 then 'https://www.smartone.com/SmarTone-CARE/intro/english/index.html?redirect=home'
else null end as partner_account_binding_link,
t1.name_lang1 as partner_desc_en,
t1.name_lang2 as partner_desc_tc,
t1.name_lang3 as partner_desc_sc,
0 as have_year_round_offer,
1 as partner_sequence_number
FROM partner t1
left join brand_category t2
on case when t1.partner_id = 5001 then 2
when t1.partner_id = 5005 then 189
when t1.partner_id = 5004 then 19
else t1.partner_id end
= t2.brand_category_id
""")
brands_preaction = "Delete from data_migration.public.gt_brands;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(brand, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_brands",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_brands",
    additional_options={"preactions": brands_preaction}
    )   

# brand cat link
brand_cat_link = spark.sql("""select
Distinct case when brand_id = 2 then 5001 when brand_id = 19 then 5004 when brand_id = 189 then 5005 else brand_id end as brand_id,
brand_category_id
from brand
where status = 'A'
""")

brands_brand_cat_links_preaction = "Delete from data_migration.public.gt_brands_brand_cat_links;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(brand_cat_link, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_brands_brand_cat_links",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_brands_brand_cat_links",
    additional_options={"preactions": brands_brand_cat_links_preaction}
    )   

shop = spark.sql("""SELECT
t1.id as id,
t1.shop_name_en as name_en,
t1.shop_name_tc as name_tc,
t1.shop_name_sc as name_sc,
case when t2.phase_lang1 is not null then t2.phase_lang1 else '' end as location_en,
case when t2.phase_lang2 is not null then t2.phase_lang2 else '' end as location_tc,
case when t2.phase_lang3 is not null then t2.phase_lang3 else '' end as location_sc,
case when t1.open_hour_en is not null then t1.open_hour_en else '' end as opening_hour_en,
case when t1.open_hour_tc is not null then t1.open_hour_tc else '' end as opening_hour_tc,
case when t1.open_hour_sc is not null then t1.open_hour_sc else '' end as opening_hour_sc,
case when t1.phone is not null then t1.phone else '' end as phone,
case when t4.partner_id = 5003 then t4.partner_shop_id else null end as shop_code_for_carpark,
case when t1.ticket_type = 1 then 'EAT_EASY'
when t1.ticket_type = 2 then 'MAXIM'
when t1.ticket_type = 3 then 'kabu'
else 'none'
end as ticket_type ,
t1.eat_easy as eat_easy_restaurant_id,
t1.agreement_no_1,
t1.agreement_no_2,
t1.agreement_no_3,
case when (case when t2.join_point_dollar =1 then '1' else 0 end) = '1' then 1 else 0 end as join_point_dollar,
case when t2.join_ecoupon_usage =1 then 1 else 0 end as join_ecoupon,
0 as is_featured,
ifnull(t2.created_date, now()) as created_at,
ifnull(t2.updated_date, now()) as updated_at,
now() as published_at,
1 as created_by_id,
1 as updated_by_id,
case when t2.join_member_promotion_plan =1 then 1 else 0 end as earn_bonus_point,
case when t2.passcode is not null then t2.passcode else '' end as merchant_app_q_rcode,
case when t1.status = 1 then 1 else 0 end as display_in_directory,
t2.shop_no as shop_no,
t1.mall_id as mall_id,
case when t3.partner_id = 5001 then t3.partner_shop_id else null end as yata_shop_code,
case when t6.partner_id = 5004 then t6.partner_shop_id else null end as smar_tone_shop_code,
'' as kmb_shop_code,
'' as hsbc_shop_code,
'' as go_royal_shop_code,
'' as black_box_shop_code,
case when t5.partner_id = 5007 then t5.partner_shop_id else '' end as merchant_app_shop_code,
case when t2.status = 'A' then 'ACTIVE'
when t2.status = 'V' then 'INVALID'
else 'INACTIVE' end as status
FROM cms_shop t1
left join vip_shop t2 on t1.id = t2.shop_id
left join (select * from vip_partner_shop where partner_id in (5001)) t3 on t1.id = t3.shop_id
left join (select * from vip_partner_shop where partner_id in (5003)) t4 on t1.id = t4.shop_id
left join (select * from vip_partner_shop where partner_id in (5007)) t5 on t1.id = t5.shop_id
left join (select * from vip_partner_shop where partner_id in (5004)) t6 on t1.id = t6.shop_id
left join cms_mall t7
on t1.mall_id = t7.id
left join vip_mall t7
on t1.mall_id = t7.cms_mall_id



union all

SELECT
t2.shop_id as id,
t2.name_lang1 as name_en,
t2.name_lang2 as name_tc,
t2.name_lang3 as name_sc,
case when t2.phase_lang1 is not null then t2.phase_lang1 else '' end as location_en,
case when t2.phase_lang2 is not null then t2.phase_lang2 else '' end as location_tc,
case when t2.phase_lang3 is not null then t2.phase_lang3 else '' end as location_sc,
'' as opening_hour_en,
'' as opening_hour_tc,
'' as opening_hour_sc,
'' as phone,
case when t4.partner_id = 5003 then t4.partner_shop_id else null end as shop_code_for_carpark,
'none' as ticket_type,
null as eat_easy_restaurant_id,
t2.agreement_no1,
t2.agreement_no2,
t2.agreement_no3,
case when (case when t2.join_point_dollar =1 then '1' else 0 end) = '1' then 1 else 0 end as join_point_dollar,
case when t2.join_ecoupon_usage =1 then 1 else 0 end as join_ecoupon,
0 as is_featured,
t2.created_date as created_at,
t2.updated_date as updated_at,
now() as published_at,
1 as created_by_id,
1 as updated_by_id,
case when t2.join_member_promotion_plan =1 then 1 else 0 end as earn_bonus_point,
t2.passcode as merchant_app_q_rcode,
0 as display_in_directory,
t2.shop_no as shop_no,
t2.mall_id as mall_id,
case when t3.partner_id = 5001 then t3.partner_shop_id else null end as yata_shop_code,
case when t6.partner_id = 5004 then t6.partner_shop_id else null end as smar_tone_shop_code,
'' as kmb_shop_code,
'' as hsbc_shop_code,
'' as go_royal_shop_code,
'' as black_box_shop_code,
case when t5.partner_id = 5007 then t5.partner_shop_id else '' end as merchant_app_shop_code,
case when t2.status = 'A' then 'ACTIVE'
when t2.status = 'V' then 'INVALID'
else 'INACTIVE' end as status
FROM vip_shop t2
left join cms_shop t1 on t1.id = t2.shop_id
left join (select * from vip_partner_shop where partner_id in (5001)) t3 on t1.id = t3.shop_id
left join (select * from vip_partner_shop where partner_id in (5003)) t4 on t1.id = t4.shop_id
left join (select * from vip_partner_shop where partner_id in (5007)) t5 on t1.id = t5.shop_id
left join (select * from vip_partner_shop where partner_id in (5004)) t6 on t1.id = t6.shop_id
where t1.id is null
""")
shops_preaction = "Delete from public.gt_shops;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(shop, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_shops",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_shops",
    additional_options={"preactions": shops_preaction}
    )   

shops_floor_link = spark.sql("""
Select * from (
select
shop_id,
cast(floor_plan_id as int) as floor_plan_id,
shop_order
from (
select id as shop_id, substring_index(replace(floor,' ',''),',',1) as floor_plan_id, 1 as shop_order FROM cms_shop where status = 1
union all
select id as shop_id, substring_index(substring_index(replace(floor,' ',''),',',2),',',-1) as floor_plan_id, 2 as shop_order FROM cms_shop where status = 1
union all
select id as shop_id, substring_index(substring_index(replace(floor,' ',''),',',3),',',-1) as floor_plan_id, 3 as shop_order FROM cms_shop where status = 1
union all
select id as shop_id, substring_index(substring_index(replace(floor,' ',''),',',4),',',-1) as floor_plan_id, 4 as shop_order FROM cms_shop where status = 1
union all
select id as shop_id, substring_index(substring_index(replace(floor,' ',''),',',5),',',-1) as floor_plan_id, 5 as shop_order FROM cms_shop where status = 1
)a
where length(floor_plan_id) >1
)x
where floor_plan_id not in (146,171,252,253)
""")

shops_floor_links_preaction = "Delete from public.gt_shops_floor_links;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(shops_floor_link, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_shops_floor_links",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_shops_floor_links",
    additional_options={"preactions": shops_floor_links_preaction}
    )   

shop_main_cat_link = spark.sql("""select
shop_id,
cast(shop_main_category_id as int) as shop_main_category_id,
shop_order
from (
select id as shop_id, substring_index(replace(shop_main_cats,' ',''),',',1) as shop_main_category_id, 1 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',2),',',-1) as shop_main_category_id, 2 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',3),',',-1) as shop_main_category_id, 3 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',4),',',-1) as shop_main_category_id, 4 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',5),',',-1) as shop_main_category_id, 5 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',6),',',-1) as shop_main_category_id, 6 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',7),',',-1) as shop_main_category_id, 7 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',8),',',-1) as shop_main_category_id, 8 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',9),',',-1) as shop_main_category_id, 9 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',10),',',-1) as shop_main_category_id, 10 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',11),',',-1) as shop_main_category_id, 11 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',12),',',-1) as shop_main_category_id, 12 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',13),',',-1) as shop_main_category_id, 13 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',14),',',-1) as shop_main_category_id, 14 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',15),',',-1) as shop_main_category_id, 15 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',16),',',-1) as shop_main_category_id, 16 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',17),',',-1) as shop_main_category_id, 17 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',18),',',-1) as shop_main_category_id, 18 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',19),',',-1) as shop_main_category_id, 19 as shop_order FROM cms_shop where status = 1
union all 
select id as shop_id, substring_index(substring_index(replace(shop_main_cats,' ',''),',',20),',',-1) as shop_main_category_id, 20 as shop_order FROM cms_shop where status = 1
)a
where length(shop_main_category_id) >1""")

shops_shop_main_cat_links_preaction = "Delete from public.gt_shops_shop_main_cat_links;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(shop_main_cat_link, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_shops_shop_main_cat_links",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_shops_shop_main_cat_links",
    additional_options={"preactions": shops_shop_main_cat_links_preaction}
    )   
    

shop_sub_cat_link = spark.sql("""select
shop_id,
cast(shop_sub_category_id as int) as shop_sub_category_id,
shop_order
from (
select id as shop_id, substring_index(replace(shop_cats,' ',''),',',1) as shop_sub_category_id, 1 as shop_order FROM cms_shop where status = 1
union all
select id as shop_id, substring_index(substring_index(replace(shop_cats,' ',''),',',2),',',-1) as shop_sub_category_id, 2 as shop_order FROM cms_shop where status = 1
union all
select id as shop_id, substring_index(substring_index(replace(shop_cats,' ',''),',',3),',',-1) as shop_sub_category_id, 3 as shop_order FROM cms_shop where status = 1
union all
select id as shop_id, substring_index(substring_index(replace(shop_cats,' ',''),',',4),',',-1) as shop_sub_category_id, 4 as shop_order FROM cms_shop where status = 1
union all
select id as shop_id, substring_index(substring_index(replace(shop_cats,' ',''),',',5),',',-1) as shop_sub_category_id, 5 as shop_order FROM cms_shop where status = 1
union all
select id as shop_id, substring_index(substring_index(replace(shop_cats,' ',''),',',6),',',-1) as shop_sub_category_id, 6 as shop_order FROM cms_shop where status = 1
union all
select id as shop_id, substring_index(substring_index(replace(shop_cats,' ',''),',',7),',',-1) as shop_sub_category_id, 7 as shop_order FROM cms_shop where status = 1
union all
select id as shop_id, substring_index(substring_index(replace(shop_cats,' ',''),',',8),',',-1) as shop_sub_category_id, 8 as shop_order FROM cms_shop where status = 1
union all
select id as shop_id, substring_index(substring_index(replace(shop_cats,' ',''),',',9),',',-1) as shop_sub_category_id, 9 as shop_order FROM cms_shop where status = 1
union all
select id as shop_id, substring_index(substring_index(replace(shop_cats,' ',''),',',10),',',-1) as shop_sub_category_id, 10 as shop_order FROM cms_shop where status = 1
)a
where length(shop_sub_category_id) >=1""")

shops_shop_sub_cat_links_preaction = "Delete from public.gt_shops_shop_sub_cat_links;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(shop_sub_cat_link, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_shops_shop_sub_cat_links",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_shops_shop_sub_cat_links",
    additional_options={"preactions": shops_shop_sub_cat_links_preaction}
    )   
    
gt_member = spark.sql("""select
id,
member_id,
852 as country_code,
'' as phone_number,
concat(password) as password_credential,
'' as biometric_credential,
0 as login_attempt_count,
create_date,
update_date as last_update_date,
'' as access_token,
'' as refresh_token,
0 as authorize_attempt_count,
'' as partner_access_token
from member
where status not in ('V')
""")

iam_member_preaction = "Delete from public.gt_iam_member;"

glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(gt_member, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "data_migration_public_gt_iam_member",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "data_migration_public_gt_iam_member",
    additional_options={"preactions": iam_member_preaction}
    )   