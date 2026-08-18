# Commercial Rent Yield Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest house.kg commercial *rental* listings, estimate each commercial *sale* listing's payback period / rental yield from its 3 nearest rental comps, and expose it on the commercial Grafana dashboard: click a sale marker → map narrows to that object + its 3 comps, a stat panel shows the numbers, a dashboard link resets to the full view.

**Architecture:** Rental ingestion is a new Lambda folded sequentially into the existing commercial Step Function branch (ingest sale → ingest rent → one Glue job reads both bronze tables). The Glue job computes a cross-dataset geographic k-NN (3 nearest rental comps per sale listing, reusing the haversine pattern already in `price_model.py`) and derives `payback_months`/`monthly_yield_pct`, storing them in `commercial_price_fact` (SCD2) alongside the existing `expected_price`. The Grafana dashboard adds a `selected_slug` variable, a click-driven filter on the map, a second "rental comps" layer, and a stat panel — implemented as a Python script mutating the dashboard JSON (the file has no local Grafana instance to click-test against).

**Tech Stack:** Python 3.10, PySpark (AWS Glue 5.0), boto3, Terraform (AWS provider), Grafana dashboard JSON (v2 schema, `elements`/`layout`/`variables`).

**Spec:** `docs/superpowers/specs/2026-08-18-commercial-rent-yield-design.md`

## Global Constraints

- Bbox for the new ingestion lambda: `lat1=42.529879066020332&lon1=74.01283264160158&lat2=43.096546175778314&lon2=75.08399963378908` (the unified bbox already shared by the other 3 lambdas) — **not** the wide bbox from the user's example URL.
- Rental filter: `type_id=["2"]`, `category="3"` (same category as commercial sale, different type_id).
- Comp selection: pure geographic 3-nearest, no `commercial_type` filter.
- Yield formula: `est_monthly_rent = avg(3 nearest rental comps' rent/m²) × sale object's own square`; `payback_months = (sqm_price × square) / est_monthly_rent`; `monthly_yield_pct = est_monthly_rent / (sqm_price × square) × 100`.
- No local pyspark/boto3/requests in this dev environment — Python-only files (Lambda, dashboard-mutation script) get real local tests; PySpark files (`price_model.py`, `commercial_feature_engineering.py`) are verified by a real `aws glue start-job-run` after deploy, matching this repo's existing (pre-this-feature) practice of not unit-testing Spark DataFrame logic locally.
- `terraform` and `aws` CLI are available locally and authenticated (region `eu-central-1`); use `terraform validate` (not `apply`) as the automatic check in IaC tasks — actual `apply` happens once, in the deploy task, with the user's awareness.

---

### Task 1: Rental ingestion Lambda

**Files:**
- Create: `etl/jobs/lambda/commercial_rent_ingestion/main.py`
- Create: `etl/jobs/lambda/commercial_rent_ingestion/test_main.py`
- Modify: `etl/jobs/pack.sh`

**Interfaces:**
- Produces: `handler(event, context)` — same contract as `commercial_ingestion/main.py`'s handler (reads `BUCKET_NAME`/`CRAWLER_NAME`/`FILE_NAME_PREFIX` env vars, writes `s3://{BUCKET_NAME}/ingestions_commercial_rent/{FILE_NAME_PREFIX}-{DDMMYYYY}.json`, retargets the named Glue crawler to that key). Task 2's Terraform wires this Lambda's env vars to `FILE_NAME_PREFIX=commercial_rent`, `CRAWLER_NAME=commercial_rent_ingestions_crawler`.

- [ ] **Step 1: Write the failing test**

Create `etl/jobs/lambda/commercial_rent_ingestion/test_main.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd etl/jobs/lambda/commercial_rent_ingestion && python3 test_main.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'main'` (the module doesn't exist yet).

- [ ] **Step 3: Write minimal implementation**

Create `etl/jobs/lambda/commercial_rent_ingestion/main.py` (clone of `../commercial_ingestion/main.py` with the bbox kept as-is — it's already the shared unified bbox — and `type_id` value changed from `1` to `2`, plus the S3/log message text updated for rent):

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd etl/jobs/lambda/commercial_rent_ingestion && python3 test_main.py`
Expected: `OK: rent ingestion targets type_id=2, shared bbox, correct S3 key/crawler retarget`

- [ ] **Step 5: Wire into pack.sh**

In `etl/jobs/pack.sh`, after the existing `commercial_ingestion` block, append:

```bash
cd ../commercial_rent_ingestion
zip -j ../../../../iac/artifacts/commercial_rent_ingestion_lambda.zip main.py
```

Full resulting file:

```bash
#!/bin/bash
set -e
cd lambda/ingestion/layer
zip -r ../../../../../iac/artifacts/requests_layer.zip python
cd ..
zip -j ../../../../iac/artifacts/ingestion_lambda.zip main.py
cd ../plots_ingestion
zip -j ../../../../iac/artifacts/plots_ingestion_lambda.zip main.py
cd ../commercial_ingestion
zip -j ../../../../iac/artifacts/commercial_ingestion_lambda.zip main.py
cd ../commercial_rent_ingestion
zip -j ../../../../iac/artifacts/commercial_rent_ingestion_lambda.zip main.py
```

- [ ] **Step 6: Commit**

```bash
git add etl/jobs/lambda/commercial_rent_ingestion/main.py etl/jobs/lambda/commercial_rent_ingestion/test_main.py etl/jobs/pack.sh
git commit -m "feat: add commercial rent ingestion lambda"
```

---

### Task 2: IaC — Lambda resource, IAM, Glue crawler

**Files:**
- Modify: `iac/lambda.tf`
- Modify: `iac/glue.tf`

**Interfaces:**
- Consumes: `etl/jobs/lambda/commercial_rent_ingestion/main.py` (Task 1) via the packaged zip `artifacts/commercial_rent_ingestion_lambda.zip`.
- Produces: `aws_lambda_function.commercial_rent_ingestion_lambda`, `aws_glue_crawler.commercial_rent_ingestions_crawler` — both referenced by name in Task 3's Step Function JSON.

- [ ] **Step 1: Add the Lambda resource and S3 write permission**

In `iac/lambda.tf`, extend the `lambda_role_policy`'s `s3:PutObject` resource list (currently ends at `"${aws_s3_bucket.data_bucket.arn}/ingestions_commercial/*"`):

```hcl
      {
        Action = ["s3:PutObject"],
        Effect = "Allow",
        Resource = [
          "${aws_s3_bucket.data_bucket.arn}/ingestions_apartments/*",
          "${aws_s3_bucket.data_bucket.arn}/ingestions_plots/*",
          "${aws_s3_bucket.data_bucket.arn}/ingestions_commercial/*",
          "${aws_s3_bucket.data_bucket.arn}/ingestions_commercial_rent/*"
        ]
      },
```

Then, right after the existing `resource "aws_lambda_function" "commercial_ingestion_lambda"` block, add:

```hcl
resource "aws_lambda_function" "commercial_rent_ingestion_lambda" {
  function_name = "commercial_rent_ingestion_lambda"
  filename      = "artifacts/commercial_rent_ingestion_lambda.zip"
  handler       = "main.handler"
  memory_size   = 512
  runtime       = "python3.10"
  role          = aws_iam_role.lambda_role.arn
  source_code_hash = filebase64sha256("artifacts/commercial_rent_ingestion_lambda.zip")
  layers        = [aws_lambda_layer_version.requests_layer.arn]
  timeout       = 60
  environment {
    variables = {
      BUCKET_NAME      = var.s3_bucket
      FILE_NAME_PREFIX = "commercial_rent"
      CRAWLER_NAME     = "commercial_rent_ingestions_crawler"
    }
  }
}
```

- [ ] **Step 2: Add the Glue crawler**

In `iac/glue.tf`, right after the existing `resource "aws_glue_crawler" "commercial_ingestions_crawler"` block, add:

```hcl
resource "aws_glue_crawler" "commercial_rent_ingestions_crawler" {
  name          = "commercial_rent_ingestions_crawler"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.data_db.name
  classifiers   = [aws_glue_classifier.housekg_json_classifier.name]

  s3_target {
    path = "s3://${aws_s3_bucket.data_bucket.bucket}/ingestions_commercial_rent/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Tables = { AddOrUpdateBehavior = "MergeNewColumns" }
    }
  })
}
```

(No new IAM permissions needed for the crawler or the Glue job to read this new prefix/table — `glue_crawler_role` already has `s3:GetObject`/`s3:ListBucket` on the whole bucket and `glue:CreateTable`/`GetTable` etc. on `"*"`; `glue_job_role` already has `s3:*` on the whole bucket and `glue:*` on `"*"`.)

- [ ] **Step 3: Validate**

Run: `cd iac && terraform validate`
Expected: `Success! The configuration is valid.`

- [ ] **Step 4: Commit**

```bash
git add iac/lambda.tf iac/glue.tf
git commit -m "feat: add terraform resources for commercial rent ingestion"
```

---

### Task 3: IaC — Step Function branch wiring

**Files:**
- Modify: `iac/sf.tf`

**Interfaces:**
- Consumes: `aws_lambda_function.commercial_rent_ingestion_lambda`, `aws_glue_crawler.commercial_rent_ingestions_crawler` (Task 2).

- [ ] **Step 1: Insert the rent ingest/crawl states into the commercial branch**

In `iac/sf.tf`, inside the commercial branch's state machine definition, replace:

```json
            "CommercialCrawlerStatusChoice": {
              "Type": "Choice",
              "Choices": [
                {
                  "Variable": "$.Crawler.State",
                  "StringEquals": "RUNNING",
                  "Next": "WaitCommercialCrawler"
                }
              ],
              "Default": "StartCommercialGlueJob"
            },
            "StartCommercialGlueJob": {
```

with:

```json
            "CommercialCrawlerStatusChoice": {
              "Type": "Choice",
              "Choices": [
                {
                  "Variable": "$.Crawler.State",
                  "StringEquals": "RUNNING",
                  "Next": "WaitCommercialCrawler"
                }
              ],
              "Default": "IngestCommercialRent"
            },
            "IngestCommercialRent": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "OutputPath": "$.Payload",
              "Parameters": {
                "FunctionName": "${aws_lambda_function.commercial_rent_ingestion_lambda.function_name}",
                "Payload.$": "$"
              },
              "Next": "RunCommercialRentIngestionCrawler"
            },
            "RunCommercialRentIngestionCrawler": {
              "Type": "Task",
              "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
              "Parameters": {
                "Name": "${aws_glue_crawler.commercial_rent_ingestions_crawler.name}"
              },
              "Next": "WaitCommercialRentCrawler"
            },
            "WaitCommercialRentCrawler": {
              "Type": "Wait",
              "Seconds": 60,
              "Next": "CheckCommercialRentCrawler"
            },
            "CheckCommercialRentCrawler": {
              "Type": "Task",
              "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler",
              "Parameters": {
                "Name": "${aws_glue_crawler.commercial_rent_ingestions_crawler.name}"
              },
              "Next": "CommercialRentCrawlerStatusChoice"
            },
            "CommercialRentCrawlerStatusChoice": {
              "Type": "Choice",
              "Choices": [
                {
                  "Variable": "$.Crawler.State",
                  "StringEquals": "RUNNING",
                  "Next": "WaitCommercialRentCrawler"
                }
              ],
              "Default": "StartCommercialGlueJob"
            },
            "StartCommercialGlueJob": {
```

- [ ] **Step 2: Extend the Step Function role's lambda invoke permission**

In `iac/sf.tf`, in `aws_iam_role_policy.step_function_policy`, extend:

```hcl
        Resource = [
          aws_lambda_function.ingestion_lambda.arn,
          aws_lambda_function.plots_ingestion_lambda.arn,
          aws_lambda_function.commercial_ingestion_lambda.arn,
          aws_lambda_function.commercial_rent_ingestion_lambda.arn
        ]
```

- [ ] **Step 3: Validate Terraform syntax**

Run: `cd iac && terraform validate`
Expected: `Success! The configuration is valid.`

- [ ] **Step 4: Validate the embedded state machine JSON is well-formed**

The `${...}` interpolations sit inside quoted JSON string values, so the heredoc content is valid JSON as plain text (before Terraform ever substitutes them). Run:

```bash
cd iac
awk '/definition = <<EOF/{flag=1; next} /^EOF$/{flag=0} flag' sf.tf | python3 -m json.tool > /dev/null && echo "JSON OK"
```

Expected: `JSON OK`. If this fails, the most likely cause is a trailing comma or mismatched brace introduced by the edit in Step 1 — fix and re-run.

- [ ] **Step 5: Commit**

```bash
git add iac/sf.tf
git commit -m "feat: wire commercial rent ingestion into the commercial step function branch"
```

---

### Task 4: `price_model.py` — cross-dataset nearest comps

**Files:**
- Modify: `etl/jobs/glue/price_model.py`

**Interfaces:**
- Consumes: two PySpark `DataFrame`s, each with a `slug` column and the given `lat_col`/`lon_col`/`price_col`.
- Produces: `nearest_cross_comps(sale_df, comp_df, *, price_col, k=3, lat_col="latitude", lon_col="longitude") -> DataFrame` — returns `sale_df` with added columns `comp{1..k}_slug` (string), `comp{1..k}_lat`/`comp{1..k}_lon`/`comp{1..k}_rent_sqm_price` (double), and `avg_rent_sqm_price` (double, mean of the found comps' `price_col`). All added columns are null for a sale row with null lat/lon, or for every sale row if `comp_df` has zero rows with coordinates.

- [ ] **Step 1: Implement**

Add to `etl/jobs/glue/price_model.py` (after `adjust_expected_price_for_flag`):

```python
def nearest_cross_comps(
    sale_df: DataFrame,
    comp_df: DataFrame,
    *,
    price_col: str,
    k: int = 3,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
) -> DataFrame:
    """For each row in `sale_df`, find its `k` geographically nearest rows in
    `comp_df` — a DIFFERENT dataset, unlike `add_expected_price`'s self-join, so no
    self-exclusion is needed. Adds flattened `comp{1..k}_slug/lat/lon/rent_sqm_price`
    columns (ordered nearest-first) plus `avg_rent_sqm_price` (mean of the found
    comps' `price_col`).

    All added columns are null for a sale row with null `lat_col`/`lon_col`, and for
    every sale row if `comp_df` has no rows with coordinates at all — same
    "degrades gracefully, no min-row guard" philosophy as `add_expected_price`; the
    caller decides whether a null estimate is meaningful downstream.
    """

    def _null_comp_columns(df: DataFrame) -> DataFrame:
        out = df
        for i in range(1, k + 1):
            out = (
                out.withColumn(f"comp{i}_slug", F.lit(None).cast("string"))
                .withColumn(f"comp{i}_lat", F.lit(None).cast("double"))
                .withColumn(f"comp{i}_lon", F.lit(None).cast("double"))
                .withColumn(f"comp{i}_rent_sqm_price", F.lit(None).cast("double"))
            )
        return out.withColumn("avg_rent_sqm_price", F.lit(None).cast("double"))

    comp_has_coords = comp_df.filter(F.col(lat_col).isNotNull() & F.col(lon_col).isNotNull())
    if comp_has_coords.count() == 0:
        return _null_comp_columns(sale_df)

    sale_has_coords = sale_df.filter(F.col(lat_col).isNotNull() & F.col(lon_col).isNotNull())
    sale_no_coords = sale_df.filter(F.col(lat_col).isNull() | F.col(lon_col).isNull())

    a = sale_has_coords.select("slug", lat_col, lon_col).alias("a")
    b = comp_has_coords.select(
        F.col("slug").alias("b_slug"),
        F.col(lat_col).alias("b_lat"),
        F.col(lon_col).alias("b_lon"),
        F.col(price_col).cast("double").alias("b_price"),
    )

    lat1, lat2 = F.radians(F.col(f"a.{lat_col}")), F.radians(F.col("b_lat"))
    dlat = lat2 - lat1
    dlon = F.radians(F.col("b_lon") - F.col(f"a.{lon_col}"))
    hav = F.sin(dlat / 2) ** 2 + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2) ** 2
    dist_km = F.lit(2 * _EARTH_RADIUS_KM) * F.asin(F.sqrt(hav))

    ranked = (
        a.crossJoin(b)
        .withColumn("_dist", dist_km)
        .withColumn(
            "_rank",
            F.row_number().over(Window.partitionBy(F.col("a.slug")).orderBy(F.col("_dist").asc())),
        )
        .filter(F.col("_rank") <= k)
    )

    agg_cols = []
    for i in range(1, k + 1):
        agg_cols.append(F.max(F.when(F.col("_rank") == i, F.col("b_slug"))).alias(f"comp{i}_slug"))
        agg_cols.append(F.max(F.when(F.col("_rank") == i, F.col("b_lat"))).alias(f"comp{i}_lat"))
        agg_cols.append(F.max(F.when(F.col("_rank") == i, F.col("b_lon"))).alias(f"comp{i}_lon"))
        agg_cols.append(F.max(F.when(F.col("_rank") == i, F.col("b_price"))).alias(f"comp{i}_rent_sqm_price"))
    agg_cols.append(F.avg("b_price").alias("avg_rent_sqm_price"))

    pivoted = ranked.groupBy(F.col("a.slug").alias("slug")).agg(*agg_cols)

    with_comps = sale_has_coords.join(pivoted, "slug", "left")
    return with_comps.unionByName(_null_comp_columns(sale_no_coords))
```

- [ ] **Step 2: Syntax-check (no local pyspark to run it against real data)**

Run: `python3 -m py_compile etl/jobs/glue/price_model.py`
Expected: no output, exit code 0.

This function's actual correctness (the haversine distance math, the rank-based pivot) is verified in Task 6 against a real Glue job run — no pyspark is installed in this dev environment, and the existing `add_expected_price`/`adjust_expected_price_for_flag` in this same file have never had local unit tests for the same reason.

- [ ] **Step 3: Commit**

```bash
git add etl/jobs/glue/price_model.py
git commit -m "feat: add cross-dataset nearest-comps kNN to price_model"
```

---

### Task 5: `commercial_feature_engineering.py` — wire in rent comps and yield

**Files:**
- Modify: `etl/jobs/glue/commercial_feature_engineering.py`

**Interfaces:**
- Consumes: `nearest_cross_comps` (Task 4); Glue Catalog table `commercial_rent_<DDMMYYYY>_json` (Task 1-3, populated after a real pipeline run).
- Produces: `commercial_price_fact` gains `est_monthly_rent`, `payback_months`, `monthly_yield_pct`, `comp{1,2,3}_slug/lat/lon/rent_sqm_price` (SCD2-tracked, same table Task 7's Grafana queries read from).

- [ ] **Step 1: Import the new function and add the rent bronze table name**

In `etl/jobs/glue/commercial_feature_engineering.py`, change:

```python
from price_model import add_expected_price, adjust_expected_price_for_flag
```

to:

```python
from price_model import add_expected_price, adjust_expected_price_for_flag, nearest_cross_comps
```

And right after the existing line `bronze_table_name = f"commercial_{timestamp_str}_json"`, add:

```python
bronze_rent_table_name = f"commercial_rent_{timestamp_str}_json"
```

- [ ] **Step 2: Add `get_bronze_rent_df()`**

Add this function right after `get_bronze_df()`:

```python
def get_bronze_rent_df():
    df_bronze = glue_context.create_dynamic_frame.from_catalog(
        database=glue_database_name,
        table_name=bronze_rent_table_name
    ).toDF().select(F.explode(F.col("list")).alias("house")).select('house.*')

    square_double = F.coalesce(
        F.col("square.double"),
        F.col("square.int").cast(T.DoubleType())
    )

    df_bronze = df_bronze.select(
        F.col('slug'),
        F.col('longitude'),
        F.col('latitude'),
        F.col('prices')[1]['m2_price'].cast(T.DoubleType()).alias('sqm_price'),
        square_double.alias('square'),
    )

    return (
        df_bronze
        .dropDuplicates(["slug"])
        .filter(F.col("sqm_price") > 0)
        .filter(F.col("square") > 0)
        .filter(F.col("latitude").isNotNull())
        .filter(F.col("longitude").isNotNull())
    )
```

- [ ] **Step 3: Wire rent comps and yield into `main()`**

Replace the current `main()`:

```python
def main():
    bronze_df = get_bronze_df().alias("incoming")
    bronze_df.cache()

    corrected_df = apply_anomaly_corrections(bronze_df)
    cleaned_df_bronze = apply_strict_filters(corrected_df)
    cleaned_df_bronze.cache()

    scored_df = add_expected_price(cleaned_df_bronze, price_col="sqm_price")
    scored_df = adjust_expected_price_for_flag(scored_df, price_col="sqm_price", flag_col="is_basement")
    scored_df.cache()

    commercial_dim_df = scored_df.drop("sqm_price", "expected_price", "price_vs_expected_pct")
    commercial_dim_df.write.mode("overwrite").parquet(commercial_dim_table_s3_uri)

    new_rows = find_new_rows(spark, price_fact_s3_uri, cleaned_df_bronze.select("slug", "square", "sqm_price"))
    qualifying_slugs = [
        r["slug"] for r in
        new_rows.filter(
            (F.col("sqm_price") * F.col("square") >= 120000) & (F.col("sqm_price") * F.col("square") <= 200000)
        ).select("slug").collect()
    ]
    send_webhook(qualifying_slugs, webhook_url)

    update_scd2_table(
        key_fields=[T.StructField("slug", T.StringType(), False)],
        parquet_path=price_fact_s3_uri,
        compared_fields=[
            T.StructField("sqm_price", T.DoubleType(), False),
            T.StructField("expected_price", T.DoubleType(), True),
            T.StructField("price_vs_expected_pct", T.DoubleType(), True),
        ],
        comparison_df=scored_df.select(
            "slug", "sqm_price", "expected_price", "price_vs_expected_pct", F.col("updated_at").alias("timestamp")
        ),
        partition_col="is_current"
    )

    create_commercial_market_summary_table(cleaned_df_bronze)


if __name__ == "__main__":
    main()
```

with:

```python
_RENT_COMP_COLUMNS = [
    f"comp{i}_{field}"
    for i in (1, 2, 3)
    for field in ("slug", "lat", "lon", "rent_sqm_price")
] + ["avg_rent_sqm_price", "est_monthly_rent", "payback_months", "monthly_yield_pct"]


def main():
    bronze_df = get_bronze_df().alias("incoming")
    bronze_df.cache()

    corrected_df = apply_anomaly_corrections(bronze_df)
    cleaned_df_bronze = apply_strict_filters(corrected_df)
    cleaned_df_bronze.cache()

    scored_df = add_expected_price(cleaned_df_bronze, price_col="sqm_price")
    scored_df = adjust_expected_price_for_flag(scored_df, price_col="sqm_price", flag_col="is_basement")

    rent_df = get_bronze_rent_df()
    scored_df = nearest_cross_comps(scored_df, rent_df, price_col="sqm_price", k=3)
    scored_df = (
        scored_df
        .withColumn(
            "est_monthly_rent",
            F.when(F.col("avg_rent_sqm_price").isNotNull(), F.col("avg_rent_sqm_price") * F.col("square")),
        )
        .withColumn(
            "payback_months",
            F.when(
                F.col("est_monthly_rent").isNotNull() & (F.col("est_monthly_rent") > 0),
                (F.col("sqm_price") * F.col("square")) / F.col("est_monthly_rent"),
            ),
        )
        .withColumn(
            "monthly_yield_pct",
            F.when(
                F.col("est_monthly_rent").isNotNull() & (F.col("square") > 0),
                F.col("est_monthly_rent") / (F.col("sqm_price") * F.col("square")) * 100.0,
            ),
        )
    )
    scored_df.cache()

    commercial_dim_df = scored_df.drop(
        "sqm_price", "expected_price", "price_vs_expected_pct", *_RENT_COMP_COLUMNS
    )
    commercial_dim_df.write.mode("overwrite").parquet(commercial_dim_table_s3_uri)

    new_rows = find_new_rows(spark, price_fact_s3_uri, cleaned_df_bronze.select("slug", "square", "sqm_price"))
    qualifying_slugs = [
        r["slug"] for r in
        new_rows.filter(
            (F.col("sqm_price") * F.col("square") >= 120000) & (F.col("sqm_price") * F.col("square") <= 200000)
        ).select("slug").collect()
    ]
    send_webhook(qualifying_slugs, webhook_url)

    update_scd2_table(
        key_fields=[T.StructField("slug", T.StringType(), False)],
        parquet_path=price_fact_s3_uri,
        compared_fields=[
            T.StructField("sqm_price", T.DoubleType(), False),
            T.StructField("expected_price", T.DoubleType(), True),
            T.StructField("price_vs_expected_pct", T.DoubleType(), True),
            T.StructField("est_monthly_rent", T.DoubleType(), True),
            T.StructField("payback_months", T.DoubleType(), True),
            T.StructField("monthly_yield_pct", T.DoubleType(), True),
            T.StructField("comp1_slug", T.StringType(), True),
            T.StructField("comp1_lat", T.DoubleType(), True),
            T.StructField("comp1_lon", T.DoubleType(), True),
            T.StructField("comp1_rent_sqm_price", T.DoubleType(), True),
            T.StructField("comp2_slug", T.StringType(), True),
            T.StructField("comp2_lat", T.DoubleType(), True),
            T.StructField("comp2_lon", T.DoubleType(), True),
            T.StructField("comp2_rent_sqm_price", T.DoubleType(), True),
            T.StructField("comp3_slug", T.StringType(), True),
            T.StructField("comp3_lat", T.DoubleType(), True),
            T.StructField("comp3_lon", T.DoubleType(), True),
            T.StructField("comp3_rent_sqm_price", T.DoubleType(), True),
        ],
        comparison_df=scored_df.select(
            "slug", "sqm_price", "expected_price", "price_vs_expected_pct",
            "est_monthly_rent", "payback_months", "monthly_yield_pct",
            "comp1_slug", "comp1_lat", "comp1_lon", "comp1_rent_sqm_price",
            "comp2_slug", "comp2_lat", "comp2_lon", "comp2_rent_sqm_price",
            "comp3_slug", "comp3_lat", "comp3_lon", "comp3_rent_sqm_price",
            F.col("updated_at").alias("timestamp")
        ),
        partition_col="is_current"
    )

    create_commercial_market_summary_table(cleaned_df_bronze)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Syntax-check**

Run: `python3 -m py_compile etl/jobs/glue/commercial_feature_engineering.py`
Expected: no output, exit code 0.

- [ ] **Step 5: Commit**

```bash
git add etl/jobs/glue/commercial_feature_engineering.py
git commit -m "feat: compute rental yield/payback in commercial_feature_engineering"
```

---

### Task 6: Deploy and verify end-to-end

**Files:** none (operational task)

**Interfaces:**
- Consumes: everything from Tasks 1–5.
- Produces: a populated `commercial_rent_<date>_json` Glue Catalog table and a `commercial_price_fact` table with the new columns — what Task 7's dashboard queries against.

- [ ] **Step 1: Push the module/script changes to S3 and apply Terraform**

```bash
cd etl/jobs && ./pack.sh
cd ../../iac && terraform apply
```

Review the plan before confirming — expect only additive resources (1 new lambda, 1 new crawler, modified `sf.tf`/`lambda.tf` policies, updated `price_model_module`/`commercial_feature_engineering_script` S3 objects). Confirm with the user before typing `yes` if anything unexpected shows up (e.g. resource replacement/deletion).

- [ ] **Step 2: Run the pipeline once, end-to-end, outside the daily cron**

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:eu-central-1:975050012362:stateMachine:data-processing-workflow \
  --region eu-central-1
```

Poll with `aws stepfunctions describe-execution --execution-arn <arn>` (or `get-execution-history`) until it finishes. Expect ~5-8 minutes total (commercial branch now has one more ingest+crawl round-trip).

- [ ] **Step 3: Verify the rent bronze table landed with the assumed price semantics**

```bash
aws glue get-table --database-name realty_data --name commercial_rent_<DDMMYYYY>_json --region eu-central-1
```

Then spot-check a few real rows via Athena (workgroup `housekg_workgroup`) — confirm `prices[1].m2_price` values for rental listings look like a plausible monthly rent/m² (roughly 3-30 depending on type, not a total-price-scale number like sale's 100-8000). If the assumption from the spec's Risk #1 is wrong, fix `get_bronze_rent_df()`'s price extraction (Task 5, Step 2) and re-run this task.

- [ ] **Step 4: Verify `commercial_price_fact`'s schema picked up the new columns**

Per the repo's documented schema-propagation gotcha, the crawler must run once after the job writes new columns — the Step Function's `RunCommercialPriceFactCrawler` already does this as part of `RunCommercialFinalCrawlers`, so it should already be current. Confirm:

```bash
aws glue get-table --database-name realty_data --name commercial_price_fact --region eu-central-1 \
  --query 'Table.StorageDescriptor.Columns[].Name'
```

Expected: includes `payback_months`, `monthly_yield_pct`, `est_monthly_rent`, `comp1_slug` (and the other comp columns).

- [ ] **Step 5: No commit** (operational verification only — nothing to add to git).

---

### Task 7: Grafana dashboard — click-to-drill-down yield view

**Files:**
- Create (scratch, not committed): a one-off Python script to mutate the JSON — write it directly into `grafana-dashboard-commercial.json` via the Edit tool instead if that's simpler once you're looking at the real file; the script below is the precise transformation either way.
- Modify: `grafana-dashboard-commercial.json`

**Interfaces:**
- Consumes: `commercial_price_fact` columns from Task 5/6 (`payback_months`, `monthly_yield_pct`, `est_monthly_rent`, `comp1..3_slug/lat/lon/rent_sqm_price`).

- [ ] **Step 1: Add the `selected_slug` variable**

In `grafana-dashboard-commercial.json`, in the top-level `variables` array, add (matching the existing `TextVariable` shape used by `sqm_price_min` etc.):

```json
{
  "kind": "TextVariable",
  "spec": {
    "current": { "text": "", "value": "" },
    "description": "Slug of the sale object clicked on the map; empty = show all",
    "hide": "",
    "label": "Selected object",
    "name": "selected_slug",
    "query": "",
    "skipUrlSync": false
  }
}
```

- [ ] **Step 2: Filter the Listings layer's query by `selected_slug`**

In `elements["panel-10"].spec.data.spec.queries[0].spec.query.spec.rawSQL`, the query currently ends with:

```sql
WHERE c.price_vs_expected_pct IS NOT NULL
ORDER BY price_vs_expected_pct ASC;
```

Change the final `SELECT ... FROM current c WHERE ...` to also select the new columns and filter by the variable:

```sql
SELECT
    c.latitude,
    c.longitude,
    c.slug,
    c.micro_district,
    c.commercial_type,
    c.square,
    c.fact_sqm_price,
    c.fact_expected_sqm_price,
    c.price_vs_expected_pct,
    c.price,
    c.payback_months,
    c.monthly_yield_pct,
    c.est_monthly_rent
FROM current c
WHERE (${selected_slug} = '' OR c.slug = '${selected_slug}')
ORDER BY price_vs_expected_pct ASC;
```

and add `f.payback_months`, `f.monthly_yield_pct`, `f.sqm_price * r.square / NULLIF(f.est_monthly_rent, 0) AS payback_months_check` — no, keep it minimal: just add `f.payback_months, f.monthly_yield_pct, f.est_monthly_rent` to the `current AS (...)` CTE's `SELECT` list (it currently selects `f.sqm_price * r.square AS price, ROUND(f.sqm_price) AS fact_sqm_price, ...` from `commercial_price_fact f JOIN commercial_dim r`). The full CTE becomes:

```sql
WITH current AS (
    SELECT
        r.micro_district,
        r.commercial_type,
        r.slug AS slug,
        r.square,
        r.land_square,
        r.latitude,
        r.longitude,
        f.sqm_price * r.square AS price,
        ROUND(f.sqm_price) AS fact_sqm_price,
        ROUND(f.expected_price) AS fact_expected_sqm_price,
        ROUND(f.price_vs_expected_pct, 1) AS price_vs_expected_pct,
        f.payback_months,
        f.monthly_yield_pct,
        f.est_monthly_rent
    FROM commercial_price_fact f
    JOIN commercial_dim r
        ON f.slug = r.slug
    WHERE f.is_current = true
        AND r.micro_district IN (${district:csv})
        AND r.commercial_type IN (${commercial_type:singlequote})
        AND r.square BETWEEN ${square_min} AND ${square_max}
        AND (r.land_square IS NULL OR r.land_square BETWEEN ${land_square_min} AND ${land_square_max})
        AND (r.floor IS NULL OR r.floor BETWEEN ${floor_min} AND ${floor_max})
        AND (r.floors IS NULL OR r.floors BETWEEN ${floors_min} AND ${floors_max})
        AND (r.year IS NULL OR r.year BETWEEN ${year_min} AND ${year_max})
        AND f.sqm_price BETWEEN ${sqm_price_min} AND ${sqm_price_max}
)
SELECT
    c.latitude,
    c.longitude,
    c.slug,
    c.micro_district,
    c.commercial_type,
    c.square,
    c.fact_sqm_price,
    c.fact_expected_sqm_price,
    c.price_vs_expected_pct,
    c.price,
    c.payback_months,
    c.monthly_yield_pct,
    c.est_monthly_rent
FROM current c
WHERE (${selected_slug} = '' OR c.slug = '${selected_slug}')
ORDER BY price_vs_expected_pct ASC;
```

(Dropping the old `WHERE c.price_vs_expected_pct IS NOT NULL` is deliberate — that field is no longer the color driver, and excluding nulls there would also hide any commercial object that happens to lack a `price_vs_expected_pct`, which is unrelated to whether it should show on the map.)

- [ ] **Step 3: Replace the color scheme with the 6-step payback-in-years threshold**

In `elements["panel-10"].spec.vizConfig.spec.fieldConfig.defaults`, replace the `thresholds` block:

```json
"thresholds": {
  "mode": "absolute",
  "steps": [
    { "color": "dark-green", "value": 0 },
    { "color": "green", "value": 61 },
    { "color": "yellow", "value": 73 },
    { "color": "orange", "value": 85 },
    { "color": "red", "value": 97 },
    { "color": "dark-red", "value": 109 }
  ]
}
```

And in `elements["panel-10"].spec.vizConfig.spec.options.layers[0].config.style.color`, change the field driving the color from `price_vs_expected_pct` to `payback_months`:

```json
"color": {
  "field": "payback_months",
  "fixed": "dark-green"
}
```

- [ ] **Step 4: Add the drill-down data link on the `slug` field**

In `elements["panel-10"].spec.vizConfig.spec.fieldConfig.overrides`, the existing entry matching `{"id": "byName", "options": "slug"}` has a `properties` array with one `links` property (the "View Details" house.kg link). Add a second link object to that same `value` array:

```json
{
  "targetBlank": false,
  "title": "Show yield & comps",
  "url": "?var-selected_slug=${__value.raw}"
}
```

(so the `links` property's `value` array now has two entries: the existing "View Details" and this new one).

- [ ] **Step 5: Add the "Rental comps" layer**

Add a second query to the panel (so the map has two independently-queryable result sets) and a second layer that reads it. In `elements["panel-10"].spec.data.spec.queries`, append a second `PanelQuery` (refId `B`):

```json
{
  "kind": "PanelQuery",
  "spec": {
    "hidden": false,
    "query": {
      "datasource": { "name": "afgo4scn4wu0wa" },
      "group": "grafana-athena-datasource",
      "kind": "DataQuery",
      "spec": {
        "connectionArgs": {
          "catalog": "__default",
          "database": "__default",
          "region": "__default",
          "resultReuseEnabled": false,
          "resultReuseMaxAgeInMinutes": 60
        },
        "format": 1,
        "rawSQL": "SELECT comp1_lat AS latitude, comp1_lon AS longitude, comp1_slug AS slug, comp1_rent_sqm_price AS rent_sqm_price FROM commercial_price_fact WHERE is_current = true AND slug = '${selected_slug}' AND comp1_slug IS NOT NULL\nUNION ALL\nSELECT comp2_lat, comp2_lon, comp2_slug, comp2_rent_sqm_price FROM commercial_price_fact WHERE is_current = true AND slug = '${selected_slug}' AND comp2_slug IS NOT NULL\nUNION ALL\nSELECT comp3_lat, comp3_lon, comp3_slug, comp3_rent_sqm_price FROM commercial_price_fact WHERE is_current = true AND slug = '${selected_slug}' AND comp3_slug IS NOT NULL;\n",
        "table": "commercial_price_fact"
      },
      "version": "v0"
    },
    "refId": "B"
  }
}
```

Then in `elements["panel-10"].spec.vizConfig.spec.options.layers`, append a second layer, styled distinctly (fixed blue, square symbol, no threshold coloring) and pinned to query `B` via `filterData` (Grafana's standard field-matcher mechanism, already used elsewhere in this dashboard for the "View Details" `byName` override — `byFrameRefID` is the matcher id Grafana Geomap uses to pick a specific query's result for a layer):

```json
{
  "config": {
    "showLegend": true,
    "style": {
      "color": { "fixed": "blue" },
      "opacity": 0.8,
      "size": { "fixed": 6, "max": 6, "min": 6 },
      "symbol": { "fixed": "img/icons/marker/square.svg", "mode": "fixed" }
    }
  },
  "filterData": { "id": "byFrameRefID", "options": "B" },
  "location": { "mode": "auto" },
  "name": "Rental comps",
  "tooltip": true,
  "type": "markers"
}
```

**Manual verification needed** (no live Grafana available in this dev environment): after deploying this dashboard, open it, click a sale marker, and confirm the "Rental comps" layer actually shows only that object's 3 comps. If `filterData`/`byFrameRefID` isn't the right field in the Grafana version actually running, use the panel editor's own layer "Data" picker to select query `B` for this layer, then copy the JSON it produces back into this file (the panel editor always writes a working config, even if the exact hand-authored key above turns out stale for that Grafana version).

- [ ] **Step 6: Add the yield stat panel**

Add a new element (find the next free `panel-N` id — currently the highest used id is 10, so use `panel-11`) to `elements`:

```json
"panel-11": {
  "kind": "Panel",
  "spec": {
    "data": {
      "kind": "QueryGroup",
      "spec": {
        "queries": [
          {
            "kind": "PanelQuery",
            "spec": {
              "hidden": false,
              "query": {
                "datasource": { "name": "afgo4scn4wu0wa" },
                "group": "grafana-athena-datasource",
                "kind": "DataQuery",
                "spec": {
                  "connectionArgs": {
                    "catalog": "__default",
                    "database": "__default",
                    "region": "__default",
                    "resultReuseEnabled": false,
                    "resultReuseMaxAgeInMinutes": 60
                  },
                  "format": 1,
                  "rawSQL": "SELECT ROUND(payback_months, 1) AS \"Payback (months)\", ROUND(monthly_yield_pct, 2) AS \"Monthly yield (%)\", ROUND(est_monthly_rent) AS \"Est. monthly rent (USD)\" FROM commercial_price_fact WHERE is_current = true AND slug = '${selected_slug}';\n",
                  "table": "commercial_price_fact"
                },
                "version": "v0"
              },
              "refId": "A"
            }
          }
        ],
        "queryOptions": {},
        "transformations": []
      }
    },
    "description": "Payback/yield for the selected object, estimated from its 3 nearest rental comps",
    "id": 11,
    "links": [],
    "title": "Доходность",
    "vizConfig": {
      "group": "stat",
      "kind": "VizConfig",
      "spec": {
        "fieldConfig": {
          "defaults": {
            "color": { "mode": "thresholds" },
            "thresholds": { "mode": "absolute", "steps": [{ "color": "text", "value": 0 }] },
            "unit": "none"
          },
          "overrides": []
        },
        "options": {
          "colorMode": "value",
          "graphMode": "none",
          "justifyMode": "auto",
          "orientation": "auto",
          "reduceOptions": { "calcs": ["lastNotNull"], "fields": "", "values": false },
          "textMode": "auto"
        }
      },
      "version": "13.1.2"
    }
  }
}
```

Then add it to the layout — find the `Metrics` tab's `GridLayout.spec.items` array (the one holding `panel-1`, `panel-4`, etc.) and append:

```json
{
  "kind": "GridLayoutItem",
  "spec": {
    "element": { "kind": "ElementReference", "name": "panel-11" },
    "height": 5,
    "width": 12,
    "x": 12,
    "y": 18
  }
}
```

(matching `panel-2`'s `y: 18, x: 0` neighbor already in that grid — this places it beside it.)

- [ ] **Step 7: Add the reset dashboard link**

Set the top-level `links` array (currently `[]`) to:

```json
[
  {
    "asDropdown": false,
    "icon": "external link",
    "includeVars": false,
    "keepTime": false,
    "tags": [],
    "targetBlank": false,
    "title": "Показать все объекты",
    "tooltip": "",
    "type": "link",
    "url": "?var-selected_slug="
  }
]
```

- [ ] **Step 8: Validate the JSON**

Run: `python3 -m json.tool grafana-dashboard-commercial.json > /dev/null && echo "JSON OK"`
Expected: `JSON OK`.

Then run a structural sanity check:

```bash
python3 -c "
import json
d = json.load(open('grafana-dashboard-commercial.json'))
names = {v['spec']['name'] for v in d['variables']}
assert 'selected_slug' in names, 'selected_slug variable missing'
map_panel = d['elements']['panel-10']['spec']
assert len(map_panel['data']['spec']['queries']) == 2, 'expected 2 queries on the map panel'
assert len(map_panel['vizConfig']['spec']['options']['layers']) == 2, 'expected 2 map layers'
thresholds = map_panel['vizConfig']['spec']['fieldConfig']['defaults']['thresholds']['steps']
assert [s['color'] for s in thresholds] == ['dark-green', 'green', 'yellow', 'orange', 'red', 'dark-red']
assert 'panel-11' in d['elements'], 'stat panel missing'
assert d['links'][0]['url'] == '?var-selected_slug=', 'reset link missing'
print('OK: structural checks pass')
"
```

Expected: `OK: structural checks pass`.

- [ ] **Step 9: Manual click-through (cannot be done from this dev environment)**

After the dashboard JSON is imported/updated in the real Grafana instance: click a sale marker → confirm the map narrows to it + up to 3 blue square comp markers, the stat panel fills in, and clicking the "Показать все объекты" dashboard link resets the view. Report back anything that doesn't behave as expected — most likely candidate is the `filterData`/`byFrameRefID` layer-to-query binding from Step 5.

- [ ] **Step 10: Commit**

```bash
git add grafana-dashboard-commercial.json
git commit -m "feat: add rental yield drill-down to the commercial dashboard map"
```

---

## Self-review notes

- **Spec coverage:** ingestion (Task 1-3), yield computation/storage (Task 4-5), deploy/verify (Task 6), Grafana variable/filter/color/comps-layer/stat-panel/reset-link (Task 7, Steps 1-7) — all spec sections have a task.
- **Type consistency:** `nearest_cross_comps`'s output column names (`comp{i}_slug/lat/lon/rent_sqm_price`, `avg_rent_sqm_price`) match exactly between Task 4 (produces) and Task 5 (consumes) and Task 7 (SQL reads the persisted `comp{i}_*` names from `commercial_price_fact`, not `avg_rent_sqm_price` which is intentionally not persisted).
- **Known open risk carried into Task 6/7** (from the spec): whether `prices[1].m2_price` means rent/m²/month for rental listings — Task 6 Step 3 is the checkpoint for this; if wrong, it blocks Task 7 from showing sane numbers and Task 5 needs a fix first.
