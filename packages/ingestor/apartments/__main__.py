import io
import json
import os

import requests
from datetime import datetime
from databricks.sdk import WorkspaceClient
import tracemalloc

tracemalloc.start()
url = "https://www.house.kg/search-map?lat1=42.718768102606326&lon1=74.36988830566408&lat2=42.90765713919232&lon2=74.72694396972658&filter=%7B%22type_id%22%3A%7B%22operator%22%3A%22in%22%2C%22value%22%3A%5B%221%22%5D%7D%2C%22category%22%3A%7B%22operator%22%3A%22%3D%22%2C%22value%22%3A%221%22%7D%2C%22document%22%3A%7B%22operator%22%3A%22in%22%2C%22value%22%3A%5B%224%22%5D%7D%7D&disable_groups=1&offset=0&page=1&mobile_view=0"

# Header to mimic browser
headers = {'X-Requested-With': 'XMLHttpRequest'}


def main(args):
    timestamp = datetime.now().strftime("%d%m%Y")

    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise error if request fails
    # Step 2: Upload to Databricks DBFS
    api_client = WorkspaceClient(
        host="https://dbc-dabd0c59-cabd.cloud.databricks.com",
        token=os.environ.get('DATABRICKS_API_KEY')  # personal
    )
    json_to_store = json.dumps(response.json()['list']).encode()
    del response
    file_name = f"apartments_{timestamp}.json"
    api_client.files.upload(
        f"/Volumes/housekg/etl/ingestions/{file_name}",
        io.BytesIO(json_to_store),
        overwrite=True
    )
    del json_to_store
