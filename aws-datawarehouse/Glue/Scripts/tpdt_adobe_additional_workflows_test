import pandas as pd
import boto3
import time
import datetime
import json


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
        time.sleep(10)


def clean_s3(s3_bucket, s3_folder):
    """clean all objects in folder"""
    s3 = boto3.resource('s3')
    
    clean_bucket = s3.Bucket(s3_bucket)
    for obj in clean_bucket.objects.filter(Prefix=s3_folder):
        s3.Object(s3_bucket,obj.key).delete()


def renaming_output(root_folder, tablename):
    # renaming config
    s3 = boto3.client('s3',region_name='ap-southeast-1')
    BucketName = 'tpdt-athena-queryresult'
    FolderName = f'{root_folder}/'
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
    rename_file = boto3.resource('s3',region_name='ap-southeast-1')
    rename_file.Object(BucketName,NewFile).copy_from(CopySource=OldSource)
    rename_file.Object(BucketName,OldFile).delete()




# Main Function
bucket = 'tpdt-adobe'
DATABASE = 'tpdt_03adobe'
max_execution=5
target_views = ['various_binding_status_for_adobe_workflow']

for target_view in target_views:
    # Config
    folder = f'to_adobe/{target_view}/raw/'
    output_path = f"s3://{bucket}/{folder}"
    query_string = f'Select * from {DATABASE}.{target_view}'
        
    # Athena to S3
    execution_id = run_athena(query_string, DATABASE, output_path)
    final_path = get_filename(execution_id, max_execution)

    

    