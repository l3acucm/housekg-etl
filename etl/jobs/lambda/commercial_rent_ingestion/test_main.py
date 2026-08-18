"""Stdlib-only self-check for commercial_rent_ingestion/main.py — stubs
requests/boto3 in sys.modules (neither is installed in this dev env), no
network or AWS calls. Run: python3 test_main.py (from this directory).
"""
import os
import sys
import types
from datetime import datetime
from unittest.mock import MagicMock

captured = {}


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _fake_get(url, headers=None, timeout=None):
    captured["url"] = url
    captured["headers"] = headers
    return _FakeResponse({"list": [{"slug": "rent-1"}]})


requests_stub = types.ModuleType("requests")
requests_stub.get = _fake_get
requests_stub.RequestException = Exception
sys.modules["requests"] = requests_stub

s3_client = MagicMock()
glue_client = MagicMock()


def _fake_boto3_client(service_name, *args, **kwargs):
    return {"s3": s3_client, "glue": glue_client}[service_name]


boto3_stub = types.ModuleType("boto3")
boto3_stub.client = _fake_boto3_client
boto3_stub.exceptions = types.SimpleNamespace(Boto3Error=Exception)
sys.modules["boto3"] = boto3_stub

os.environ["BUCKET_NAME"] = "test-bucket"
os.environ["CRAWLER_NAME"] = "commercial_rent_ingestions_crawler"
os.environ["FILE_NAME_PREFIX"] = "commercial_rent"

from main import handler  # noqa: E402

result = handler({}, None)
assert result["statusCode"] == 200

assert "lat1=42.529879066020332" in captured["url"]
assert "lon1=74.01283264160158" in captured["url"]
assert "lat2=43.096546175778314" in captured["url"]
assert "lon2=75.08399963378908" in captured["url"]
assert "%5B%222%22%5D" in captured["url"], "type_id filter must select rentals (2), not sale (1)"
assert "%5B%221%22%5D" not in captured["url"], "must not accidentally reuse the sale filter"
assert captured["headers"] == {"X-Requested-With": "XMLHttpRequest"}

today = datetime.now().strftime("%d%m%Y")
expected_key = f"ingestions_commercial_rent/commercial_rent-{today}.json"

put_kwargs = s3_client.put_object.call_args.kwargs
assert put_kwargs["Bucket"] == "test-bucket"
assert put_kwargs["Key"] == expected_key

update_kwargs = glue_client.update_crawler.call_args.kwargs
assert update_kwargs["Name"] == "commercial_rent_ingestions_crawler"
assert update_kwargs["Targets"]["S3Targets"][0]["Path"] == f"s3://test-bucket/{expected_key}"

print("OK: rent ingestion targets type_id=2, shared bbox, correct S3 key/crawler retarget")
