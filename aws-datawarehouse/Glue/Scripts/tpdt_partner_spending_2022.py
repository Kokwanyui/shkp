import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime, date, time, timezone,timedelta 
from pyspark.sql import Row, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import json
import time
from datetime import datetime, date, time, timezone,timedelta

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
    db03="tpdt_03dw"
    db02="tpdt_02replica"
    db03sub="tpdt_03sub"
    db03fd="tpdt_03foodordering"
    output_path="s3://tpdt-adhoc/"+table_name+"/"
    staging_path="s3://tpdt-staging/"
    dimension_path="s3://tpdt-dimension/"
    foodorder_path="s3://tpdt-foodordering/"
    db03adhoc = "tpdt_03adhoc"
else:
    db01="tpdt_01replica_"+env
    db03="tpdt_03dw_"+env
    db03sub="tpdt_03sub_"+env
    db03fd="tpdt_03foodordering"+env
    output_path="s3://tpdt-dw-"+env+"/"+table_name+"/"
    staging_path="s3://tpdt-staging-"+env+"/"
    dimension_path="s3://tpdt-dimension-"+env+"/"
    foodorder_path="s3://tpdt-foodordering-"+env+"/"


dimension_prefix = "bi_dimension_"
prefix="shkpmalls_vip_"
prefix03="tpdt_"

date_time = (datetime.now() - timedelta(30)).strftime('%Y-%m-%d')


## Source Extraction -----------------------------------------------------------------------------------------------------------------------------------------------------------
##source table : receipt
t_tpdt_partner_spending = glueContext.create_dynamic_frame.from_catalog(database = db03, table_name = 'tpdt_dw_spending', transformation_ctx = "tpdt_partner_spending")
tpdt_partner_spending_df = t_tpdt_partner_spending.toDF().filter("where receipt_upload_date >= to_date('2023-07-30') and left(spending_transaction_id) = 'ST'")




## Loading Result to S3 -----------------------------------------------------------------------------------------------------------------------------------------------------------
glueContext.write_dynamic_frame.from_catalog(
    frame = DynamicFrame.fromDF(tpdt_partner_spending_df, glueContext, "full_load"),
    database= "tpdt_03redshift",
    table_name = "tpdt_dw_staging_spending",
    catalog_connection="tpdt-03redshift-connector",
    redshift_tmp_dir="s3://tpdt-athena-queryresult",
    transformation_ctx = "tpdt_dw_staging_spending"
    )


job.commit()
## Loading Result to S3 -----------------------------------------------------------------------------------------------------------------------------------------------------------

