import boto3
import zipfile
import os
from botocore.exceptions import ClientError

LAMBDA_BUCKET_NAME = 'lambda-read-config-from-dynamo'
GLUE_JOB_BUCKET_NAME = 'glue-job-bucket-12-05'
DATA_BUCKET_NAME = 'data-dump-bucket-11-05'
GLUE_OUTPUT_BUCKET_NAME = 'glue-output-bucket-11-05'
TASK_QUEUE_NAME = 'task-queue'
DYNAMO_TABLE_NAME = 'GlueJobRules'
INFRA_STACK_NAME = 'InfrastructureCreation'
REGION_NAME = 'ap-south-1'
LAMBDA_ZIP_FILE_NAME = 'lambda_function.zip'
CSV_GLUE_FILE_PATH = 'word_count.py'
LAMBDA_HANDLER_FILE_PATH= 'lambda_function.py'
INFRA_TEMPLATE_PATH = 'infra_template.yaml'
LAMBDA_RUNTIME_VERSION = 'python3.12'
TEMP_VAR_LAMBDA_HANDLER = 'lambda_function.lambda_handler'
TEMP_VAR_CSV_GLUE_JOB_NAME = 'CsvParserJob'
TEMP_VAR_CSV_GLUE_JOB_S3_KEY = CSV_GLUE_FILE_PATH
TEMP_VAR_CSV_GLUE_SCRIPT_NAME=f's3://{GLUE_JOB_BUCKET_NAME}/{TEMP_VAR_CSV_GLUE_JOB_S3_KEY}'
TEMP_VAR_CSV_GLUE_OUTPUT_PATH = f's3://{GLUE_OUTPUT_BUCKET_NAME}/csv_output/'

Parameters=[
        {'ParameterKey': 'S3DataBucketName', 'ParameterValue': DATA_BUCKET_NAME},
        {'ParameterKey': 'TaskQueueName', 'ParameterValue': TASK_QUEUE_NAME},
        {'ParameterKey': 'LambdaRuntimeVersion', 'ParameterValue': LAMBDA_RUNTIME_VERSION},
        {'ParameterKey': 'LambdaHandler', 'ParameterValue': TEMP_VAR_LAMBDA_HANDLER},
        {'ParameterKey': 'LambdaBucketName', 'ParameterValue': LAMBDA_BUCKET_NAME},
        {'ParameterKey': 'LambdaBucketKey', 'ParameterValue': LAMBDA_ZIP_FILE_NAME},
        {'ParameterKey': 'DynamoDBTableName', 'ParameterValue': DYNAMO_TABLE_NAME},
        {'ParameterKey': 'CSVGlueJobName', 'ParameterValue': TEMP_VAR_CSV_GLUE_JOB_NAME},
        {'ParameterKey': 'CSVGlueScriptLocation', 'ParameterValue': TEMP_VAR_CSV_GLUE_SCRIPT_NAME},
        {'ParameterKey': 'GlueOutputS3BucketName', 'ParameterValue': GLUE_OUTPUT_BUCKET_NAME},
        {'ParameterKey': 'CSVGlueOutputLocation', 'ParameterValue': TEMP_VAR_CSV_GLUE_OUTPUT_PATH},
]

dynamo_db_config_items = [
        {
            'file_type': 'csv',
            'min_size_mb': 0,
            'max_size_mb': 100,
            'glue_job_name': 'CsvParserJob'
        },
        {
            'file_type': 'json',
            'min_size_mb': 0,
            'max_size_mb': 50,
            'glue_job_name': 'JsonProcessorJob'
        }
    ]


def create_bucket(s3_client, bucket_name, region):
    try:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': region},
        )
        print(f"Bucket {bucket_name} created:")
        return True
    except Exception as e:
        print(f"Error creating bucket {bucket_name}: {e}")
        return False


def zip_lambda_function():
    try:
        if os.path.exists(LAMBDA_ZIP_FILE_NAME):
            os.remove(LAMBDA_ZIP_FILE_NAME)
            print(f"Deleted local zip file: {LAMBDA_ZIP_FILE_NAME}")

        with zipfile.ZipFile(LAMBDA_ZIP_FILE_NAME, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(LAMBDA_HANDLER_FILE_PATH)
        print(f"Zipped {LAMBDA_HANDLER_FILE_PATH} to {LAMBDA_ZIP_FILE_NAME}")
        return True
    except Exception as e:
        print(f"Error zipping {LAMBDA_HANDLER_FILE_PATH}: {e}")
        return False


def upload_to_s3(s3_client, bucket_name, file_path, s3_key=None):
    if s3_key is None:
        s3_key = file_path

    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
        return True
    except Exception as e:
        print(f"Error uploading {file_path} to s3://{bucket_name}: {e}")
        return False


def create_infrastructure(cf_client, stack_name, template_body, parameters):
    try:
        response = cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=template_body,
            Parameters=parameters,
            Capabilities=['CAPABILITY_IAM'],
        )
        print(f"Stack {stack_name} creation initiated: {response}")
        stack_id = response['StackId']
         # Wait for stack creation to complete (or fail)
        waiter = cf_client.get_waiter('stack_create_complete')
        print(f"Waiting for stack '{stack_name}' to complete...")
        waiter.wait(StackName=stack_id)  # Use the Stack ID
        print(f"Stack '{stack_name}' creation completed.")
        return True
    except Exception as e:
        print(f"Error creating stack {stack_name}: {e}")
        return False


def init_dynamo_db(dynamodb_client, table_name, config_items):
    try:
        table = dynamodb_client.Table(table_name)

        for item in config_items:
            table.put_item(Item=item)
            print(f"Inserted: {item['file_type']}")

    except ClientError as e:
        print(f"Error initializing DynamoDB table: {e}")



def main() :
    s3 = boto3.client('s3', REGION_NAME)
    cf = boto3.client('cloudformation', REGION_NAME)
    dynamodb = boto3.resource('dynamodb', REGION_NAME)

    if not create_bucket(s3, LAMBDA_BUCKET_NAME, REGION_NAME):
        return

    if not create_bucket(s3, GLUE_JOB_BUCKET_NAME, REGION_NAME):
        return
    
    if not zip_lambda_function():
        return

    if not upload_to_s3(s3, LAMBDA_BUCKET_NAME, LAMBDA_ZIP_FILE_NAME):
        return
    
    if not upload_to_s3(s3, GLUE_JOB_BUCKET_NAME, CSV_GLUE_FILE_PATH, TEMP_VAR_CSV_GLUE_JOB_S3_KEY):
        return

    with open(INFRA_TEMPLATE_PATH, 'r') as f:
        template_body = f.read()

    if not create_infrastructure(cf, INFRA_STACK_NAME, template_body, Parameters):
        return

    init_dynamo_db(dynamodb, DYNAMO_TABLE_NAME, dynamo_db_config_items)


if __name__ == "__main__":
    main()