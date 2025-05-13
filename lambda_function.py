import boto3
import os
import json

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
glue = boto3.client('glue')

TABLE_NAME = os.environ['DYNAMO_TABLE_NAME'] 
OUTPUT_STORE_PATH = os.environ['GLUE_OUTPUT_LOCATION']

def lambda_handler(event, context):

   print("this is main event -> ",event)
   sqs_record = event['Records'][0]
   s3_event_str = sqs_record['body']
   s3_event = json.loads(s3_event_str)


   print("this is s3 event -> ", s3_event)
   
   s3_record = s3_event['Records'][0]
   bucket = s3_record['s3']['bucket']['name']
   key = s3_record['s3']['object']['key']
   size = s3_record['s3']['object']['size'] / (1024 * 1024)  # Convert bytes to MB
   extension = key.split('.')[-1]
   
   print(f"Bucket: {bucket}, Key: {key}, Size: {size} MB, Extension: {extension}")

   table = dynamodb.Table(TABLE_NAME)
   response = table.get_item(Key={'file_type': extension})

   print("this is response in which item is needed -> ",response)
   
   if 'Item' not in response:
      print(f"No rule found for {extension}")
      return
   
   rule = response['Item']

   print("input file path -> ",f"s3://{bucket}/{key}")

   if rule['min_size_mb'] <= size <= rule['max_size_mb']:
      job_name = rule['glue_job_name']
      
      # 4. Trigger Glue job

      job_arguments = {
         "--input_path": f"s3://{bucket}/{key}",
         "--output_path": f"{OUTPUT_STORE_PATH}/{extension}",
         "--file_size": str(size),
         "--file_extension": extension
      }

      glue.start_job_run(JobName=job_name, Arguments=job_arguments)
      print(f"Triggered Glue job: {job_name}")
   else:
      print(f"File size {size}MB out of bounds for {extension}")