import boto3
import os

def handler(event, context):
    glue = boto3.client('glue')
    job_name = os.environ['JOB_NAME']
    response = glue.start_job_run(JobName=job_name)
    return {"statusCode": 200, "jobRunId": response['JobRunId']}