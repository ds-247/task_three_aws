import boto3
import time
import os

glue = boto3.client('glue')
athena = boto3.client('athena')

DB_NAME = 'random-db-name-05-14'
CRAWLER_NAME = 'my-dynamic-crawler'
S3_TARGET_BUCKET = os.environ['S3_TARGET_BUCKET']
ROLE_FOR_CRAWLER = os.environ['ROLE_FOR_CRAWLER']
ATHENA_VIEW_BUCKET = os.environ['ATHENA_VIEW_BUCKET']


def create_database(db_name):
    try:
        glue.create_database(
            DatabaseInput={
                'Name': db_name,
                'Description': 'Glue database created by Lambda.'
            }
        )
        print(f"Database '{db_name}' created.")
    except glue.exceptions.AlreadyExistsException:
        print(f"Database '{db_name}' already exists.")


def create_crawler():
    response = glue.create_crawler(
        Name=CRAWLER_NAME,
        Role= ROLE_FOR_CRAWLER,
        DatabaseName= DB_NAME,
        Targets={'S3Targets': [{'Path': f's3://{S3_TARGET_BUCKET}/'}]},
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        }
    )

    print(f"Crawler {CRAWLER_NAME} created: {response}")


def run_crawler():
    try:
        glue.start_crawler(Name=CRAWLER_NAME)
        print("crawler is started ...")
    except glue.exceptions.CrawlerRunningException:
        print("Crawler is already running.")


def wait_for_crawler_to_finish(timeout=500):
    start_time = time.time()
    while True:
        state = glue.get_crawler(Name=CRAWLER_NAME)['Crawler']['State']
        if state == 'READY':
            print("Crawler completed.")
            return True
        if time.time() - start_time > timeout:
            print("Timeout waiting for crawler.")
            return False
        print(f"Crawler status: {state}, waiting...")
        time.sleep(5)


def get_latest_table_name():
    tables = glue.get_tables(DatabaseName=DB_NAME)['TableList']
    if not tables:
        raise Exception("No tables found in database")
    
    latest_table = sorted(tables, key=lambda x: x['CreateTime'], reverse=True)[0]
    return latest_table['Name']


def wait_for_athena_query(query_execution_id, timeout=60):
    start = time.time()
    while True:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return state
        if time.time() - start > timeout:
            return 'TIMEOUT'
        time.sleep(2)


def run_athena_query():
    table_name = get_latest_table_name()
    view_name = "dynamic_view"

    print("athena function called ..... ")
    print("table name ", table_name)

    query = f"""
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT * FROM {table_name}
    """

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DB_NAME},
        ResultConfiguration={'OutputLocation': f's3://{ATHENA_VIEW_BUCKET}/'}
    )
    query_execution_id = response['QueryExecutionId']
    print("Athena query started with ID:", query_execution_id)

    # Optional: Wait and get status
    status = wait_for_athena_query(query_execution_id)
    print("Query status:", status)



def start_crawling():
    create_database(DB_NAME)

    try:
        create_crawler()
    except glue.exceptions.AlreadyExistsException:
        print("Crawler already exists")
    
    run_crawler()

    if not wait_for_crawler_to_finish():
        return 
    
    run_athena_query()
    return {"status": "Pipeline executed successfully"}


def lambda_handler(event, context):
    print("Received event:", event)

    detail = event.get("detail", {})
    job_name = detail.get("jobName")
    state = detail.get("state")

    if state == "SUCCEEDED":
        print(f"Glue job '{job_name}' succeeded.")
        start_crawling()
        # handle success
    elif state == "FAILED":
        print(f"Glue job '{job_name}' failed.")
        # handle failure
    else:
        print(f"Glue job '{job_name}' ended with state: {state}")
        # handle other states like TIMEOUT, STOPPED, etc.
