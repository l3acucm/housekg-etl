# housekg-etl

ETL pipeline that scrapes real-estate listings from `house.kg` and lands them as a queryable silver layer on AWS. Three object kinds are processed in parallel: **apartments**, **plots** (land), and **commercial** real estate — all sale-only, all sharing the same bounding box (see below).

## Architecture

```
EventBridge (cron, daily 01:00 UTC)
        │
        ▼
Step Function: data-processing-workflow   (Parallel: apartments | plots | commercial)
        │
        ▼  per branch:
1. Lambda ingestion        → writes raw JSON to s3://<bucket>/ingestions_(apartments|plots|commercial)/<prefix>-DDMMYYYY.json
2. Glue Crawler             → catalogs the day's JSON into Glue table  (apartments|plots|commercial)_DDMMYYYY_json
3. Glue Job (Spark)        → reads bronze table, cleans/derives, writes silver parquet, POSTs new-listing webhook
4. Final crawlers (×3)     → catalog silver outputs into Glue
        │
        ▼
Athena workgroup `housekg_workgroup` queries silver tables; Grafana dashboards consume Athena.
```

State backend: S3 bucket `realty-etl-terraform-backend`, DynamoDB lock table `realty-etl-state-lock`, region `eu-central-1`. Data bucket: `housekg-etl-data`.

## Bounding box

All three lambdas now query the **same bbox** (`lat1=42.529879066020332, lon1=74.01283264160158, lat2=43.096546175778314, lon2=75.08399963378908`) — same center as the original apartments-only bbox, 3× its width and height. Apartment/commercial listing density falls off fast outside Bishkek, so this didn't meaningfully change apartments/commercial payload size; plots' bbox shrank a lot (from the whole Chui region) but plot density near the unified zone kept its payload in the same ballpark. If the bbox ever needs to change again, keep all three lambdas in sync — the whole point of unifying them was so a single zone covers all three object types.

## Repo layout

- `etl/jobs/lambda/ingestion/main.py` — apartments ingestion (`category=1`, `document=[4]` sale)
- `etl/jobs/lambda/plots_ingestion/main.py` — plots ingestion (`category=5` land, `document in [1,2,6,7,8]`)
- `etl/jobs/lambda/commercial_ingestion/main.py` — commercial ingestion (`category=3`, `type_id=[1]` sale — see note below on why this isn't `document`)
- `etl/jobs/lambda/ingestion/layer/python/` — packaged `requests` Lambda layer (shared by all three lambdas)
- `etl/jobs/glue/feature_engineering.py` — apartments Spark job
- `etl/jobs/glue/plots_feature_engineering.py` — plots Spark job
- `etl/jobs/glue/commercial_feature_engineering.py` — commercial Spark job
- `etl/jobs/glue/notify.py` — shared helper (uploaded via `--extra-py-files`, like `anomaly_correction.py`): detects genuinely-new listings (`find_new_rows`, must run *before* `update_scd2_table` overwrites the price_fact snapshot) and POSTs the webhook (`send_webhook`)
- `etl/jobs/pack.sh` — zips lambdas + requests layer into `iac/artifacts/`
- `iac/` — Terraform (lambda.tf, glue.tf, sf.tf step-function, s3.tf, athena.tf, main.tf, variables.tf)
- `grafana-dashboard.json`, `grafana-dashboard-plots.json`, `grafana-dashboard-commercial.json` — Grafana definitions for the silver tables

## Ingestion specifics

- All three lambdas hit `house.kg/search-map?...` with header `X-Requested-With: XMLHttpRequest` (required — without it the API returns the SPA shell HTML, not JSON).
- The response shape is `{offset, limit, count, list: [...]}`. `disable_groups` does **not** control whether items are flat — it only controls proximity clustering (`disable_groups=0` merges nearby pins into map clusters with `cnt`/`paid` fields). Even with `disable_groups=1`, house.kg **always** groups listings that share exact coordinates: `list[]` is an array of `{latitude, longitude, list: [...ads]}`. This is handled by two nested explodes, not one: the Glue crawler's `$[*]` json-path classifier turns the outer array into table rows (one row per coordinate-group), and `get_bronze_df()`'s own `F.explode(F.col("list"))` unpacks each group's inner ad array into individual listings. Don't "fix" this into a single explode — it's already correct for the API's actual shape (verified against real S3 ingestion files and the crawled `realty_dim` schema).
- Each item carries (selected): `slug`, `latitude`, `longitude`, `square` / `land_square` (struct `{int, double}` — real listings mix int and float across records, so the crawler infers a struct; don't assume a plain scalar), `prices` (array; index `[1]` is the USD entry — `m2_price`/`are_price` are always plain scalars, but `price` (total) is a `{int, double}` struct like square), `district`, `micro_district`, `description` (raw seller text — Russian/Kyrgyz), `document` (array of land-document codes for plots: 6 = construction, 7 = sown/agro), `commercial_type` (int, commercial only — see mapping below), plus many fields not currently selected.
- **Sale vs rent isn't always `document`.** For apartments, `document=[4]` in the filter happens to select sale listings. For commercial, there is no meaningful `document` filter — the deal type (sale vs rent) is controlled by `type_id` instead (`type_id=[1]` = sale, confirmed via the `type_id`/`rental_term` fields on the ads themselves). Don't assume `document` means "sale" for a category before checking — it's evidently category-specific.
- File name is `<prefix>-DDMMYYYY.json`. Crawler is retargeted to that day's S3 key inside the Lambda before the Step Function starts the crawler. The Glue job reads `apartments_DDMMYYYY_json` / `plots_DDMMYYYY_json` / `commercial_DDMMYYYY_json` from the Glue catalog using today's date.

## Commercial type mapping

`commercial_type` (int, 1–13) is mapped to a Russian label in `commercial_feature_engineering.py` (`COMMERCIAL_TYPE_LABELS`), matching house.kg's "тип коммерческого помещения" dropdown in site order (1-indexed) — confirmed against the real distribution of `commercial_type` values in production-sized samples, not from official docs:

1 магазины, бутики · 2 офисы · 3 торговые контейнеры · 4 сельское хозяйство · 5 рестораны, кафе, общепит · 6 отели, хостелы, гостиницы, зоны отдыха · 7 цеха, заводы, фабрики, мастерские · 8 автосервисы, автомойки, автобизнес · 9 салоны красоты · 10 медцентры, аптеки · 11 здания · 12 склады · 13 другая коммерческая недвижимость

Confirmed directly against the site's own markup (`<select name="commercial_type_multiple">` on `/kupit-kommercheskaia-nedvijimost`), not just inferred from value distribution.

Listings with no `commercial_type` set (~10% in samples) are **kept**, labeled `"не указано"` (unlike plots, which drops `purpose IS NULL` rows outright) — deliberately different from plots' convention, so don't "fix" this into a filter later.

## Silver model

All three jobs share a hand-rolled SCD2 helper (`update_scd2_table`) and emit three outputs:

- Apartments (`feature_engineering.py`):
  - `silver/realty_dim` — current snapshot of apartments minus price (latest write wins, mode=overwrite — not SCD2)
  - `silver/realty_price_fact` — SCD2 over `sqm_price` keyed by `slug`
  - `silver/market_summary` — SCD2 of district/market-wide counts, total price, total square, keyed by `slug` (where `slug=micro_district` or `slug="market"` for the global row)
- Plots (`plots_feature_engineering.py`):
  - `silver/plots_dim` — snapshot, includes a derived `purpose` column (`construction` if doc 6 only, `sown` if doc 7 only, else `other` — no longer dropped)
  - `silver/plots_price_fact` — SCD2 over `are_price` keyed by `slug`
  - `silver/plots_market_summary` — SCD2 keyed by `(slug, purpose)`
- Commercial (`commercial_feature_engineering.py`):
  - `silver/commercial_dim` — snapshot, includes `square`, `land_square`, `commercial_type` label
  - `silver/commercial_price_fact` — SCD2 over `sqm_price` (same per-m² model as apartments) keyed by `slug`
  - `silver/commercial_market_summary` — SCD2 keyed by `(slug, commercial_type)`, same district/market-wide pattern as plots' `purpose` split

SCD2 quirks: the helper computes a `<field>_change` % column for each `DoubleType` compared field; new/changed rows get current timestamps for `effective_from`, closed rows get `effective_to=now`, `is_current=false`. The helper requires that no current row matches a comparison row by key but with equal values — those become "unchanged".

## New-object webhook notification

Apartments and commercial jobs each detect listings that are genuinely new (their `slug` wasn't in `price_fact`'s pre-run `is_current=true` snapshot — `notify.find_new_rows`, must be called *before* `update_scd2_table` overwrites that snapshot) and, if any of those new listings also match a business filter, POST **one combined request per job run** to `WEBHOOK_URL` (Glue job argument, from `var.webhook_url`):

```
POST $WEBHOOK_URL
{"message": "https://house.kg/details/<slug1>\nhttps://house.kg/details/<slug2>\n..."}
```

- Apartments: `square > 60` and `sqm_price * square < 120000` (USD).
- Commercial: `120000 <= sqm_price * square <= 200000` (USD).
- No POST at all if nothing matches, or on a job's first-ever run (no prior `price_fact` snapshot to diff against — everything would look "new").
- Plots has no webhook — not requested.

## Hardcoded cleaning filters

These run inside the bronze→silver step and drop rows outright:

- Apartments: `sqm_price` ∈ (300, 2500); `micro_district IS NOT NULL`. Imputes `kitchen_square`, `toilet`, `ceiling_height` from `square`.
- Plots: `are_price > 0`; `land_square > 0`; `micro_district IS NOT NULL`. `purpose` is **not** filtered — plots whose `document` codes don't cleanly indicate construction-only or sown-only are labeled `other` and kept; the price band (3000, 100000) applies to both `construction` and `other` (only `sown` is exempt — agro land is legitimately cheap).
- Commercial: `sqm_price` ∈ (100, 8000) (derived from real p1–p99 distribution — median ~1700, p99 ~5500); `micro_district IS NOT NULL`. `commercial_type` is **not** filtered — unset values are kept and labeled `"не указано"` (see the commercial type mapping section above).

> Known data-quality issue: some listings have wrong `square` / `land_square` / `price` in the structured fields but state the correct values in `description` (e.g. listing 55538857: structured `land_square=1 are`, actual `3 ha` per description). These rows either get filtered out or pollute the silver layer with implausible per-unit prices.

## Conventions

- Region `eu-central-1`. No multi-region.
- All compute is serverless — no EC2, no EMR. Glue jobs use `glue_version=5.0`, `worker_type=G.1X`, 2 workers, FLEX execution class.
- Glue script source lives in repo; `aws_s3_object` resources upload it to S3 on each `terraform apply` (etag = sha of the file).
- The terraform-apply workflow needs the `iac/artifacts/` directory pre-populated (`./etl/jobs/pack.sh`, run from `etl/jobs/` — its paths are relative to that directory).
- The data bucket has `force_destroy = true` — be careful.
- `var.webhook_url` must be set (e.g. via `terraform.tfvars` or `-var`) — it's not defaulted, since it's an external, semi-sensitive endpoint.

## Setup (from README)

1. Create DynamoDB `realty-etl-state-lock` with `LockID` (String) — backend lock table.
2. `mkdir -p iac/artifacts`
3. `pip3 install -r etl/jobs/lambda/ingestion/requirements.txt -t etl/jobs/lambda/ingestion/layer/python`
4. `./etl/jobs/pack.sh` to build the lambda + layer zips
5. `cd iac && terraform init && terraform apply`

## Operational gotchas

- Lambda timeout/memory: apartments 60s/768MB, plots 60s/512MB, commercial 60s/512MB. All three were sized against real payloads at the unified bbox (~42MB, ~30MB, ~21MB raw JSON respectively) — apartments was bumped from 30s/512MB when the bbox unification made the margin tighter.
- Step Function polls each ingestion crawler with `Wait 60s` then `Choice` — a stuck crawler will loop forever (no max-retries).
- Glue job reads bronze tables by today's date; if you re-run a Step Function after midnight UTC, the crawler-emitted table name will differ from what the Glue job expects.
- `grafana-dashboard.json` has an unresolved git merge conflict sitting in the working tree (pre-existing, not from this bbox/commercial work) — resolve before editing it.
