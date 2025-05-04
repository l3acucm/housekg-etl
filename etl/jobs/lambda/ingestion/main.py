import json
import requests
import boto3
import gzip
from datetime import datetime
import os
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    # Initialize AWS clients
    s3 = boto3.client("s3")

    # Configuration
    bucket_name = os.environ.get("BUCKET_NAME", "unset_bucket_name_var")
    file_name_prefix = os.environ.get("FILE_NAME_PREFIX", "unset_file_name_var")
    url = "https://www.house.kg/search-map?lat1=42.718768102606326&lon1=74.36988830566408&lat2=42.90765713919232&lon2=74.72694396972658&filter=%7B%22type_id%22%3A%7B%22operator%22%3A%22in%22%2C%22value%22%3A%5B%221%22%5D%7D%2C%22category%22%3A%7B%22operator%22%3A%22%3D%22%2C%22value%22%3A%221%22%7D%2C%22document%22%3A%7B%22operator%22%3A%22in%22%2C%22value%22%3A%5B%224%22%5D%7D%7D&disable_groups=1&offset=0&page=1&mobile_view=0"
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    timestamp = datetime.now().strftime("%d%m%Y")
    file_name = f"{file_name_prefix}-{timestamp}.json.gz"

    try:
        # Fetch data from API
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()['list']

        # Compress JSON data with gzip
        json_data = json.dumps(data).encode('utf-8')
        compressed_data = gzip.compress(json_data)

        # Store compressed data in S3
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=compressed_data,
            ContentType="application/json",
            ContentEncoding="gzip"
        )
        logger.info(f"Stored data in s3://{bucket_name}/{file_name}")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Data ingested and crawler triggered"})
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