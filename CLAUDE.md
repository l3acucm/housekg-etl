# housekg-etl

ETL pipeline that scrapes real-estate listings from `house.kg` and lands them as a queryable silver layer on AWS. Two object kinds are processed in parallel: **apartments** (sale, Bishkek city bounding box) and **plots** (sale, broader Chui region bounding box).

## Architecture

```
EventBridge (cron, daily 01:00 UTC)
        │
        ▼
Step Function: data-processing-workflow   (Parallel: apartments | plots)
        │
        ▼  per branch:
1. Lambda ingestion        → writes raw JSON to s3://<bucket>/ingestions_(apartments|plots)/<prefix>-DDMMYYYY.json
2. Glue Crawler            → catalogs the day's JSON into Glue table  (apartments|plots)_DDMMYYYY_json
3. Glue Job (Spark)        → reads bronze table, cleans/derives, writes silver parquet
4. Final crawlers (×3)     → catalog silver outputs into Glue
        │
        ▼
Athena workgroup `housekg_workgroup` queries silver tables; Grafana dashboards consume Athena.
```

State backend: S3 bucket `realty-etl-terraform-backend`, DynamoDB lock table `realty-etl-state-lock`, region `eu-central-1`. Data bucket: `housekg-etl-data`.

## Repo layout

- `etl/jobs/lambda/ingestion/main.py` — apartments ingestion (Bishkek bbox, `category=1` apartments, `document=4` sale)
- `etl/jobs/lambda/plots_ingestion/main.py` — plots ingestion (`category=5` land, `document in [1,2,6,7,8]`)
- `etl/jobs/lambda/ingestion/layer/python/` — packaged `requests` Lambda layer (shared by both lambdas)
- `etl/jobs/glue/feature_engineering.py` — apartments Spark job
- `etl/jobs/glue/plots_feature_engineering.py` — plots Spark job
- `etl/jobs/pack.sh` — zips lambdas + requests layer into `iac/artifacts/`
- `iac/` — Terraform (lambda.tf, glue.tf, sf.tf step-function, s3.tf, athena.tf, main.tf)
- `grafana-dashboard.json`, `grafana-dashboard-plots.json` — Grafana definitions for the silver tables

## Ingestion specifics

- Both lambdas hit `house.kg/search-map?...` with header `X-Requested-With: XMLHttpRequest` (required — without it the API returns the SPA shell HTML, not JSON).
- The response shape is `{offset, limit, count, list: [...]}`. With `disable_groups=1` the items are flat; otherwise items can be nested as `list[*].list[*]` (geo-grouped clusters). Lambdas persist `response.json()['list']`.
- Each item carries (selected): `slug`, `latitude`, `longitude`, `square` / `land_square` (struct `{int, double}`), `prices` (array; index `[1]` is the USD entry with `m2_price` for apartments and `are_price` for plots), `district`, `micro_district`, `description` (raw seller text — Russian/Kyrgyz), `document` (array of land-document codes for plots: 6 = construction, 7 = sown/agro), plus many fields not currently selected.
- File name is `<prefix>-DDMMYYYY.json`. Crawler is retargeted to that day's S3 key inside the Lambda before the Step Function starts the crawler. The Glue job reads `apartments_DDMMYYYY_json` / `plots_DDMMYYYY_json` from the Glue catalog using today's date.

## Silver model

Both jobs share a hand-rolled SCD2 helper (`update_scd2_table`) and emit three outputs:

- Apartments (`feature_engineering.py`):
  - `silver/realty_dim` — current snapshot of apartments minus price (latest write wins, mode=overwrite — not SCD2)
  - `silver/realty_price_fact` — SCD2 over `sqm_price` keyed by `slug`
  - `silver/market_summary` — SCD2 of district/market-wide counts, total price, total square, keyed by `slug` (where `slug=micro_district` or `slug="market"` for the global row)
- Plots (`plots_feature_engineering.py`):
  - `silver/plots_dim` — snapshot, includes a derived `purpose` column (`construction` if doc 6 only, `sown` if doc 7 only, else null)
  - `silver/plots_price_fact` — SCD2 over `are_price` keyed by `slug`
  - `silver/plots_market_summary` — SCD2 keyed by `(slug, purpose)`

SCD2 quirks: the helper computes a `<field>_change` % column for each `DoubleType` compared field; new/changed rows get current timestamps for `effective_from`, closed rows get `effective_to=now`, `is_current=false`. The helper requires that no current row matches a comparison row by key but with equal values — those become "unchanged".

## Hardcoded cleaning filters

These run inside the bronze→silver step and drop rows outright:

- Apartments: `sqm_price` ∈ (300, 2500); `micro_district IS NOT NULL`. Imputes `kitchen_square`, `toilet`, `ceiling_height` from `square`.
- Plots: `are_price > 0`; `land_square > 0`; `micro_district IS NOT NULL`; `purpose IS NOT NULL`; for `construction` plots additionally `are_price` ∈ (3000, 100000) (`sown` plots have no upper-band filter, agro land is cheap).

> Known data-quality issue: some listings have wrong `square` / `land_square` / `price` in the structured fields but state the correct values in `description` (e.g. listing 55538857: structured `land_square=1 are`, actual `3 ha` per description). These rows either get filtered out or pollute the silver layer with implausible per-unit prices.

## Conventions

- Region `eu-central-1`. No multi-region.
- All compute is serverless — no EC2, no EMR. Glue jobs use `glue_version=5.0`, `worker_type=G.1X`, 2 workers, FLEX execution class.
- Glue script source lives in repo; `aws_s3_object` resources upload it to S3 on each `terraform apply` (etag = sha of the file).
- The terraform-apply workflow needs the `iac/artifacts/` directory pre-populated (`./etl/jobs/pack.sh`).
- The data bucket has `force_destroy = true` — be careful.

## Setup (from README)

1. Create DynamoDB `realty-etl-state-lock` with `LockID` (String) — backend lock table.
2. `mkdir -p iac/artifacts`
3. `pip3 install -r etl/jobs/lambda/ingestion/requirements.txt -t etl/jobs/lambda/ingestion/layer/python`
4. `./etl/jobs/pack.sh` to build the lambda + layer zips
5. `cd iac && terraform init && terraform apply`

## Operational gotchas

- Lambda timeout is 30 s for apartments, 60 s for plots. The plots payload (~15 MB raw) is at the edge — increase memory before timeout if it starts failing.
- Step Function polls the ingestion crawler with `Wait 60s` then `Choice` — a stuck crawler will loop forever (no max-retries).
- Glue job reads bronze tables by today's date; if you re-run a Step Function after midnight UTC, the crawler-emitted table name will differ from what the Glue job expects.
