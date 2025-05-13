import boto3
import time
import os

glue = boto3.client('glue')
CRAWLER_NAME = 'my-dynamic-crawler'
ROLE_FOR_CRAWLER = os.environ['ROLE_FOR_CRAWLER']
DB_NAME = 'random-db-name-05-14'


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
        Targets={'S3Targets': [{'Path': 's3://glue-output-bucket-11-05/'}]},
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        }
    )

    print(f"Crawler {CRAWLER_NAME} created: {response}")


def run_crawler():
    try:
        glue.start_crawler(Name=CRAWLER_NAME)
    except glue.exceptions.CrawlerRunningException:
        print("Crawler is already running.")


def wait_for_crawler_to_finish(timeout=600):
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


def start_crawling():
    create_database(DB_NAME)

    try:
        create_crawler()
    except glue.exceptions.AlreadyExistsException:
        print("Crawler already exists")
    
    run_crawler()

    if not wait_for_crawler_to_finish():
        return 
    
    # run_athena_query()
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
