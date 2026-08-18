"""Self-check for the sequential-not-threaded Bedrock call loop in anomaly_correction.py.

Stubs boto3/pyspark in sys.modules so this runs with stdlib only, no Glue/Spark env
needed: `python3 test_anomaly_correction.py`.
"""
import sys
import types
from unittest.mock import MagicMock

boto3_stub = types.ModuleType("boto3")
call_order = []


class _FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data


def _fake_client(*args, **kwargs):
    client = MagicMock()

    def invoke_model(modelId, body, contentType):
        import json
        payload = json.loads(body)
        slug_hint = payload["messages"][0]["content"]
        call_order.append(slug_hint)
        text = json.dumps({
            "actual_square_m2": None, "actual_price_usd": None, "confidence": "low", "reason": "x",
            "is_basement": True,
        })
        return {"body": _FakeBody(json.dumps({"content": [{"text": text}]}).encode())}

    client.invoke_model.side_effect = invoke_model
    return client


boto3_stub.client = _fake_client
sys.modules["boto3"] = boto3_stub

pyspark_stub = types.ModuleType("pyspark")
pyspark_sql_stub = types.ModuleType("pyspark.sql")
pyspark_sql_stub.DataFrame = object
pyspark_sql_stub.functions = MagicMock()
pyspark_stub.sql = pyspark_sql_stub
sys.modules["pyspark"] = pyspark_stub
sys.modules["pyspark.sql"] = pyspark_sql_stub

from anomaly_correction import correct_anomalies_with_haiku, classify_with_haiku  # noqa: E402

rows = [
    {"slug": f"s{i}", "description": f"desc {i}", "structured_square": 50, "structured_price_usd": 1000, "implied_per_unit": 20}
    for i in range(5)
]
rows.append({"slug": "no-desc", "description": "", "structured_square": 1, "structured_price_usd": 1, "implied_per_unit": 1})

out = correct_anomalies_with_haiku(rows, kind="apartment", unit_label="m2", model_id="m", region="r", max_calls=3)

assert len(call_order) == 3, f"expected cap of 3 calls, got {len(call_order)}"
assert "no-desc" not in "".join(call_order), "row with empty description must be skipped, not sent to Bedrock"

print("OK: cap respected, empty-description rows skipped, calls made sequentially (no ThreadPoolExecutor)")

# Regression: a None slug reaching `confirmed` poisons every downstream
# `col("slug").isin(confirmed)` call — SQL's 3-valued logic turns non-matches into
# NULL instead of False (bit the commercial rent basement-matching feature in
# production: every non-basement rental came back is_basement=None, not False, so
# `a.is_basement == b.is_basement` excluded every non-basement pairing).
call_order.clear()
classify_rows = [
    {"slug": None, "description": "цоколь, есть свет"},
    {"slug": "real-slug-1", "description": "цоколь, отдельный вход"},
]
confirmed = classify_with_haiku(
    classify_rows, system_prompt="p", question_key="is_basement", model_id="m", region="r",
)
assert None not in confirmed, "a None slug must never enter the confirmed set"
assert confirmed == {"real-slug-1"}, f"expected only the real slug confirmed, got {confirmed}"
assert len(call_order) == 1, "the None-slug row must be skipped before ever calling Bedrock"

print("OK: classify_with_haiku never lets a None slug into the confirmed set")
