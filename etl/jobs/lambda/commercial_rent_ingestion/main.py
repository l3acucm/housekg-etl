import json
import requests
import boto3
from datetime import datetime
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    s3 = boto3.client("s3")
    glue_client = boto3.client('glue')

    bucket_name = os.environ.get("BUCKET_NAME", "unset_bucket_name_var")
    crawler_name = os.environ.get("CRAWLER_NAME", "unset_crawler_name_var")
    file_name_prefix = os.environ.get("FILE_NAME_PREFIX", "unset_file_name_var")
    url = "https://www.house.kg/search-map?lat1=42.529879066020332&lon1=74.01283264160158&lat2=43.096546175778314&lon2=75.08399963378908&filter=%7B%22type_id%22%3A%7B%22operator%22%3A%22in%22%2C%22value%22%3A%5B%222%22%5D%7D%2C%22category%22%3A%7B%22operator%22%3A%22%3D%22%2C%22value%22%3A%223%22%7D%7D&disable_groups=1&offset=0&page=1&mobile_view=0"
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    timestamp = datetime.now().strftime("%d%m%Y")
    file_name = f"{file_name_prefix}-{timestamp}.json"
    file_key = f"ingestions_commercial_rent/{file_name}"

    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        data = response.json()['list']

        json_data = json.dumps(data).encode('utf-8')

        s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json_data,
            ContentType="application/json"
        )
        logger.info(f"Stored data in s3://{bucket_name}/{file_key}")
        glue_client.update_crawler(
            Name=crawler_name,
            Targets={
                'S3Targets': [{'Path': f's3://{bucket_name}/{file_key}'}]
            }
        )
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Commercial rent data ingested and crawler retargeted"})
        }

    except requests.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"AWS service error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise
