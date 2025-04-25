import json
import requests
import boto3
from datetime import datetime


def handler(event, context):
    sts = boto3.client("sts")
    identity = sts.get_caller_identity()
    print("Running as:", identity)
    s3 = boto3.client("s3")
    url = "https://www.house.kg/search-map?lat1=42.718768102606326&lon1=74.36988830566408&lat2=42.90765713919232&lon2=74.72694396972658&filter=%7B%22type_id%22%3A%7B%22operator%22%3A%22in%22%2C%22value%22%3A%5B%221%22%5D%7D%2C%22category%22%3A%7B%22operator%22%3A%22%3D%22%2C%22value%22%3A%221%22%7D%2C%22document%22%3A%7B%22operator%22%3A%22in%22%2C%22value%22%3A%5B%224%22%5D%7D%7D&disable_groups=1&offset=0&page=1&mobile_view=0"

    # Header to mimic browser
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    timestamp = datetime.now().strftime("%d%m%Y")

    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise error if request fails
    json_to_store = json.dumps(response.json()['list']).encode()
    del response
    file_name = f"ingestions/apartments_{timestamp}.json"
    s3.put_object(
        Bucket="housekg-etl-bucket",  # Or pass via env var
        Key=file_name,
        Body=json_to_store,
        ContentType="application/json"
    )
