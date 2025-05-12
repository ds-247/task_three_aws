import boto3
import zipfile
import os
from botocore.exceptions import ClientError

LAMBDA_BUCKET_NAME = 'lambda-read-config-from-dynamo'
REGION_NAME = 'ap-south-1'
ZIP_FILE_NAME = 'lambda_function.zip'
LAMBDA_HANDLER_FILE_PATH= 'lambda_function.py'
DATA_BUCKET_NAME = 'data-dump-bucket-11-05'
TASK_QUEUE_NAME = 'task-queue'
STACK_NAME = 'task-three-stack-test-7'
INFRA_TEMPLATE_PATH = 'infra_template.yaml'
LAMBDA_RUNTIME_VERSION = 'python3.12'
LAMBDA_HANDLER = 'lambda_function.lambda_handler'
DYNAMO_TABLE_NAME = 'GlueJobRules'

Parameters=[
        {'ParameterKey': 'S3DataBucketName', 'ParameterValue': DATA_BUCKET_NAME},
        {'ParameterKey': 'TaskQueueName', 'ParameterValue': TASK_QUEUE_NAME},
        {'ParameterKey': 'LambdaRuntimeVersion', 'ParameterValue': LAMBDA_RUNTIME_VERSION},
        {'ParameterKey': 'LambdaHandler', 'ParameterValue': LAMBDA_HANDLER},
        {'ParameterKey': 'LambdaBucketName', 'ParameterValue': LAMBDA_BUCKET_NAME},
        {'ParameterKey': 'LamdbaBucketKey', 'ParameterValue': ZIP_FILE_NAME},
        {'ParameterKey': 'DynamoDBTableName', 'ParameterValue': DYNAMO_TABLE_NAME},
]

dynamo_db_config_items = [
        {
            'file_type': 'csv',
            'min_size_mb': 1,
            'max_size_mb': 100,
            'glue_job_name': 'CsvParserJob'
        },
        {
            'file_type': 'json',
            'min_size_mb': 5,
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
        with zipfile.ZipFile(ZIP_FILE_NAME, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(LAMBDA_HANDLER_FILE_PATH)
        print(f"Zipped {LAMBDA_HANDLER_FILE_PATH} to {ZIP_FILE_NAME}")
        return True
    except Exception as e:
        print(f"Error zipping {LAMBDA_HANDLER_FILE_PATH}: {e}")
        return False


def upload_to_s3(s3_client, bucket_name, file_path):
    try:
        s3_client.upload_file(file_path, bucket_name, file_path)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{file_path}")
    except Exception as e:
        print(f"Error uploading {file_path} to s3://{bucket_name}: {e}")
        return False
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Deleted local zip file: {file_path}")
            return True


def create_infrastructure(cf_client, stack_name, template_body, parameters):
    try:
        response = cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=template_body,
            Parameters=parameters,
            Capabilities=['CAPABILITY_IAM'],
        )
        print(f"Stack {stack_name} creation initiated: {response}")
        return True
    except Exception as e:
        print(f"Error creating stack {stack_name}: {e}")
        return False


def init_dynamo_db(dynamodb_client, table_name, config_items):
    try:
        # Wait until the table exists
        waiter = dynamodb_client.meta.client.get_waiter('table_exists')
        print(f"Waiting for table '{table_name}' to be created...")
        waiter.wait(TableName=table_name)
        print(f"Table '{table_name}' has been created.")

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
    
    if not zip_lambda_function():
        return

    if not upload_to_s3(s3, LAMBDA_BUCKET_NAME, ZIP_FILE_NAME):
        return

    with open(INFRA_TEMPLATE_PATH, 'r') as f:
        template_body = f.read()

    if not create_infrastructure(cf, STACK_NAME, template_body, Parameters):
        return
    
    init_dynamo_db(dynamodb, DYNAMO_TABLE_NAME, dynamo_db_config_items)


if __name__ == "__main__":
    main()