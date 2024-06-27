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
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType 
import pandas as pd
import numpy as np
import json
from datetime import datetime, date, time, timezone,timedelta 
from pyspark.sql.window import Window
import time


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'env'])
##Initialization
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 

##Input and Output Config
env = args['env']
if env == 'prod':
    db01="tpdt_01replica"
    db02="tpdt_02replica"
    db03="tpdt_03dw"
    db03sub="tpdt_03sub"
    db03ad="tpdt_03adobe"
    db03parking = "tpdt_03parking"
    output_path="s3://tpdt-athena-queryresult/"
    staging_path="s3://tpdt-staging/"
    dimension_path="s3://tpdt-dimension/"

else:
    db01="tpdt_01replica_"+env
    db03="tpdt_03dw_"+env
    db03sub="tpdt_03sub_"+env
    output_path="s3://tpdt-adobe-1-"+env+"/"
    staging_path="s3://tpdt-staging-"+env+"/"
    dimension_path="s3://tpdt-dimension-"+env+"/"



prefix="shkpmalls_vip_"
prefix03='tpdt_'
bi_prefix = 'bi_datamart_'
dimension_prefix = 'bi_dimension_'
suffix = datetime.now().astimezone(timezone(timedelta(hours=8))).strftime("%Y%m%d%H%M%S")
s3_folder = 'to_adobe'
"""
t_registration = glueContext.create_dynamic_frame.from_catalog(database = db01, table_name = prefix+'member_setting', transformation_ctx = "registration")
registration_df = t_registration.toDF().select('member_id', 
'receive_promotion_msg',  
'receive_promotion_sms',  
'receive_promotion_email',  
'receive_promotion_mail',  
'receive_promotion_call')

registration_df.write.format('parquet').mode('overwrite').option("header",True).save('s3://tpdt-adhoc/tpdt_member_setting/')
"""



## udfs
def clean_s3(root_folder, sub_folder):
    # clean all objects in folder
    s3 = boto3.resource('s3',region_name='ap-southeast-1')
    clean_bucket = s3.Bucket('tpdt-adobe')
    clean_prefix = f'{root_folder}/{sub_folder}'
    clean_bucket.objects.filter(Prefix=clean_prefix).delete()
    
    
def save_df(df, separator, root_folder):
    # clean tmp folder
    sub_folder='temp/'

    # save temp file
    output_path = f"s3://tpdt-staging/{root_folder}/"
    df.coalesce(1).write.mode("overwrite").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").option("dateFormat", "yyyy-MM-dd").option("compression", "gzip").csv(output_path, header="true", sep=separator)


def renaming_output(root_folder, tablename):
    # renaming config
    s3 = boto3.client('s3')
    BucketName = 'tpdt-adobe'
    FolderName = f'{root_folder}/temp/'
    FinalFolder = f'{root_folder}/'
    
    NewFile = f'{FinalFolder}{tablename}.csv.gz'
    response = s3.list_objects(
        Bucket= BucketName,
        MaxKeys = 2,
        Prefix=FolderName
        )
    OldFile=response['Contents'][0]['Key']
    OldSource=BucketName + '/' +   OldFile
    
    # move file and rename
    rename_file = boto3.resource('s3',region_name='ap-southeast-1')
    rename_file.Object(BucketName,NewFile).copy_from(CopySource=OldSource)
    rename_file.Object(BucketName,OldFile).delete()

def run_athena(script, db, output_location):
    """Execute Athena Query and get execution_id"""
    client = boto3.client('athena')
    
    execution_id = client.start_query_execution(
        QueryString=script,
        QueryExecutionContext={
            'Database': db
        },
        ResultConfiguration={
            'OutputLocation': output_location,
        }
    )['QueryExecutionId']
    
    return execution_id


def get_filename(athena_execution_id, max_execution_time):
    """Check Running Status and get final result path"""
    client = boto3.client('athena')
    state = 'RUNNING'
    
    while (max_execution_time > 0 and state in ['RUNNING', 'QUEUED']):
        max_execution_time = max_execution_time - 1
        response = client.get_query_execution(QueryExecutionId=athena_execution_id)
        
        if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                return json.loads(json.dumps(response, default=str))
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                return s3_path
        time.sleep(60)


def move_bucket(original_file, destination_bucket, destination_file_path):
    """Move file from Bucket to another in different region"""
    client = boto3.client('s3',region_name='ap-southeast-1')
    
    response = client.copy_object(
    CopySource=original_file,  # /Bucket-name/path/filename
    Bucket=destination_bucket, # Destination bucket
    Key=destination_file_path  # Destination path/filename
)


def read_s3_object(s3_bucket, key):
    client = boto3.client('s3')
    
    s3_object = s3.get_object(Bucket=s3_bucket, Key=key)
    df = pd.read_csv(io.BytesIO(s3_object['Body'].read()))
    return df


## Tables  -----------------------------------------------------------------------------------------------------------------------------------------------------------
# Athena Run Config
bucket = 'tpdt-athena-queryresult'
output_path="s3://tpdt-athena-queryresult/tpdt_dim_pre_shoot_coupon/"
DATABASE = 'tpdt_03automation'
max_execution=5
target_view = 'pre_shoot_coupon'
query_string = f'Select * from {DATABASE}.{target_view}'

# S3 Config
destination_bucket = 'tpdt-staging'
table_name = 'tpdt_dim_pre_shoot_coupon'
sep_by = ','


# Athena to adobe S3
execution_id = run_athena(query_string, DATABASE, output_path)
final_path = get_filename(execution_id, max_execution)
dataFrame = spark.read.format("csv").option("header", "true").load(final_path)
save_df(dataFrame, sep_by , table_name)
#renaming_output(table_name, table_name)









