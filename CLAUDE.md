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
   (commercial branch only) → sequential step: ingest commercial rent listings + crawl their bronze table, before the Glue job
3. Glue Job (Spark)        → reads bronze table(s), cleans/derives, writes silver parquet, POSTs new-listing webhook
4. Final crawlers (×3)     → catalog silver outputs into Glue
        │
        ▼
Athena workgroup `housekg_workgroup` queries silver tables; Grafana dashboards consume Athena.
```

The commercial branch's rent step is sequential, not a 4th parallel branch — added as another step inside the existing branch rather than a new top-level `Parallel` branch, specifically to avoid growing the surface of the no-`Catch` `Parallel` issue documented under Operational gotchas below.

State backend: S3 bucket `realty-etl-terraform-backend`, DynamoDB lock table `realty-etl-state-lock`, region `eu-central-1`. Data bucket: `housekg-etl-data`.

## Bounding box

All four lambdas now query the **same bbox** (`lat1=42.529879066020332, lon1=74.01283264160158, lat2=43.096546175778314, lon2=75.08399963378908`) — same center as the original apartments-only bbox, 3× its width and height. Apartment/commercial listing density falls off fast outside Bishkek, so this didn't meaningfully change apartments/commercial payload size; plots' bbox shrank a lot (from the whole Chui region) but plot density near the unified zone kept its payload in the same ballpark. If the bbox ever needs to change again, keep all four lambdas in sync — the whole point of unifying them was so a single zone covers all three object types (plus the commercial rent variant).

## Repo layout

- `etl/jobs/lambda/ingestion/main.py` — apartments ingestion (`category=1`, `document=[4]` sale)
- `etl/jobs/lambda/plots_ingestion/main.py` — plots ingestion (`category=5` land, `document in [1,2,6,7,8]`)
- `etl/jobs/lambda/commercial_ingestion/main.py` — commercial ingestion (`category=3`, `type_id=[1]` sale — see note below on why this isn't `document`)
- `etl/jobs/lambda/commercial_rent_ingestion/main.py` — commercial rent ingestion (`category=3`, `type_id=[2]` rental — same bbox as the others)
- `etl/jobs/lambda/ingestion/layer/python/` — packaged `requests` Lambda layer (shared by all four lambdas)
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
  - `silver/commercial_price_fact` — SCD2 over `sqm_price` (same per-m² model as apartments) keyed by `slug`; also carries `est_monthly_rent`, `payback_months`, `monthly_yield_pct`, and `comp{1,2,3}_slug/lat/lon/rent_sqm_price` (3 nearest rental comps used to estimate yield) — SCD2-tracked like everything else in this table (see Expected-price model section)
  - `silver/commercial_market_summary` — SCD2 keyed by `(slug, commercial_type)`, same district/market-wide pattern as plots' `purpose` split

SCD2 quirks: the helper computes a `<field>_change` % column for each `DoubleType` compared field; new/changed rows get current timestamps for `effective_from`, closed rows get `effective_to=now`, `is_current=false`. The helper requires that no current row matches a comparison row by key but with equal values — those become "unchanged".

## Expected-price model (geographic k-NN, not a regression)

All three jobs estimate `expected_price` per listing (`price_model.add_expected_price`, shared module like `anomaly_correction`/`notify`) as the **median price among that listing's `k` (default 15) geographically nearest OTHER listings** — great-circle distance on `latitude`/`longitude`, pure Spark SQL (self `crossJoin` + haversine + `row_number` window, no `pyspark.ml`). No district bucketing, no room/type matching — deliberately: an earlier version was a log-linear regression on `district + rooms/type + square` via `pyspark.ml` (StringIndexer→OneHotEncoder→VectorAssembler→LinearRegression, 5-fold cross-fit to avoid in-sample leakage), and it shipped a real bug — `micro_district` values with only a handful of listings gave some cross-fit folds ~0 training signal for that district's one-hot dummy, producing wild predictions (observed: $249/m² predicted for a commercial office where every real comparable in the city was $1900-2500/m²; confirmed by joining low predictions back to per-district listing counts — every offending row sat in a district with 2-15 total listings). Collapsing rare districts into an "other" bucket was considered and rejected in favor of dropping district entirely for lat/lon k-NN, which degrades gracefully (an isolated listing just gets a wider effective comp radius) instead of failing wildly. Fit is a per-run comp lookup, not a persisted/retrained model — a market-relative screener, not a forecast.

Rows with null `latitude`/`longitude` get null `expected_price` and aren't used as a neighbor for anyone else. `# ponytail:` marked in the module — the self `crossJoin` is O(n²), fine at current Bishkek listing volumes (low thousands/batch); would need geohash-bucketed blocking if a batch grows an order of magnitude.

Adds two columns tracked in `*_price_fact` (SCD2, alongside the existing price field, so they get the same `_change` history for free):

- `expected_price` — median price/unit among the listing's `k` nearest geographic comps.
- `price_vs_expected_pct` — `(actual - expected) / expected * 100`; negative = priced below its geographic comps.

Falls back to null columns (skips) below `min_rows=30` rows in a batch — guards early/sparse crawls, not meant to error the job. Not wired into the webhook filter (`notify`) — that still uses its original fixed thresholds; this is a separate, broader "underpriced vs model" signal, added as extra columns on the existing `Underestimated` Grafana panel per dashboard (not a separate panel — merged in to avoid duplicating the same district/rooms/floor filter UI twice), plus a `Underpriced by model (map)` Geomap panel colored by `price_vs_expected_pct`.

Like `anomaly_correction`/`notify`, `price_model.py` must be in `--extra-py-files` *and* uploaded via `aws_s3_object` (`price_model_module` in `s3.tf`) for every job that imports it (all three) — same `ModuleNotFoundError` failure mode noted above if either is missed.

`price_model.py` also has `nearest_cross_comps`, a cross-dataset variant of the same geographic kNN idea: instead of self-joining a dataset against itself, it finds each commercial *sale* listing's 3 nearest *rental* comps (a different dataset) to estimate rental yield/payback for `commercial_price_fact` (see Silver model above). Confirmed against real production data: a rental listing's `prices[1].m2_price` field means rent per m² per month — this was an open risk during design, now settled.

**Schema propagation gotcha (bit us once already):** adding columns to a job's output isn't enough — the Glue Catalog table schema Athena/Grafana actually query is defined by the `*_price_fact` **crawler**, not the job. After a code change adds/renames a `price_fact` column: (1) `terraform apply` to push the script, (2) run the job (`aws glue start-job-run`) so the parquet on S3 actually has the new column, (3) run the `*_price_fact` crawler (`aws glue start-crawler`) so the Catalog schema merges it in (`AddOrUpdateBehavior = "MergeNewColumns"`). Skipping step 3 gives `column ... cannot be resolved` in Grafana/Athena even though the job succeeded.

### Description-only classification (lease-right sales, basement units)

Two categorical facts that price-based anomaly detection structurally can't see, because the listing's `sqm_price` looks completely plausible either way — only the seller's free-text `description` reveals them:

- **Lease-right sales** (commercial only): the seller is selling переуступка прав аренды (assignment of a lease), not the property itself — a different asset, pollutes price comps if left in. Dropped outright (`commercial_feature_engineering.filter_lease_right_sales`).
- **Basement/semi-basement units** (apartments + commercial, not plots — plots has no "floor"): цоколь/полуподвал legitimately sells for less per m² than an above-ground floor, but the structured `floor` field is frequently null (confirmed in production — a real цоколь listing had `floor=NULL, floors=NULL`) and never distinguishes cellar from ground floor even when present. Flagged, not dropped (`flag_basements`, adds `is_basement` to the dim table), then corrected in `price_model.adjust_expected_price_for_flag`.

Both share one mechanism in `anomaly_correction.py`: `flag_keyword_candidates` (cheap regex pre-filter over `description` — `LEASE_RIGHTS_PATTERN` / `BASEMENT_PATTERN`) narrows to candidates, then `classify_with_haiku` (generic yes/no classifier, reuses `_invoke_haiku` with a per-task `system_prompt`) confirms only those candidates — same cost shape as `correct_anomalies_with_haiku`'s anomaly correction: capped at `MAX_LLM_CALLS`, never runs Haiku over every listing. `description` is kept one step longer than before through `apply_anomaly_corrections` (previously dropped in the same `.drop(...)` as `is_anomaly`) specifically so these checks can still read it; it's dropped at the very end of that function now.

**Why `is_basement` is a correction, not a drop (unlike lease-rights):** `add_expected_price`'s geographic kNN is blind to floor level by design (see that function's docstring — matching categories caused the district-sparsity bug this replaced). A basement's nearest neighbors are almost always non-basement units, so its raw kNN `expected_price` ignores the floor discount entirely and makes a fairly-priced basement look like a huge "underpriced" false positive. `adjust_expected_price_for_flag` re-derives `expected_price` for `is_basement=true` rows by the median ratio of actual price to kNN `expected_price` **among this batch's own confirmed basements** — an empirical, self-calibrating discount factor, not a hardcoded guess. Skips the correction (leaves kNN's number as-is) below `min_group_size=8` confirmed basements in a batch — same "too few examples, don't guess" reasoning as everywhere else `min_group_size` shows up in this repo.

## New-object webhook notification

Apartments and commercial jobs each detect listings that are genuinely new (their `slug` wasn't in `price_fact`'s pre-run `is_current=true` snapshot — `notify.find_new_rows`, must be called *before* `update_scd2_table` overwrites that snapshot) and, if any of those new listings also match a business filter, POST to `WEBHOOK_URL` (Glue job argument, from `var.webhook_url`):

```
POST $WEBHOOK_URL
{"message": "https://house.kg/details/<slug1>\nhttps://house.kg/details/<slug2>\n..."}
```

- Apartments: `square > 60` and `sqm_price * square < 120000` (USD).
- Commercial: `120000 <= sqm_price * square <= 200000` (USD).
- No POST at all if nothing matches, or on a job's first-ever run (no prior `price_fact` snapshot to diff against — everything would look "new").
- Plots has no webhook — not requested.
- **`WEBHOOK_URL` forwards `message` into a Telegram bot, capped at 4096 characters.** `notify.send_webhook` packs the URL list into as few newline-joined chunks as fit under that cap (`notify._chunk_lines`, `TELEGRAM_MESSAGE_LIMIT`) and POSTs each chunk separately — so a run with many qualifying new listings sends multiple requests, not one oversized one.
- Both `notify.py` and `anomaly_correction.py` must be listed in `--extra-py-files` for any job that imports them (apartments + commercial for `notify`) *and* uploaded via an `aws_s3_object` in `s3.tf` — missing either one fails the job at import time with no data written (this exact bug shipped once: `ModuleNotFoundError: No module named 'notify'`, caught via `aws glue get-job-runs`, not by `terraform validate`/`py_compile`, neither of which execute the Glue job or resolve `--extra-py-files`).

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
- **The 3 branches are not isolated.** `RunAllPipelines` is a plain `Parallel` state with no per-branch `Catch` — if any one branch fails, Step Functions aborts the *other* branches wherever they currently are, even ones that were about to succeed. Observed in production: apartments/commercial failed fast (Glue job crashed at import within ~40s); plots' crawler was still polling (its crawl took >2min) and got `TaskStateAborted` before it ever reached `StartPlotsGlueJob` — plots simply has no run for that day, not a failure. Check `get-execution-history`, not just `get-job-runs`, when a job "didn't run" — the job may never have been started. Fix would be adding `Catch`/`ResultPath` per branch so siblings run to completion independently; not done yet.
- Glue job reads bronze tables by today's date; if you re-run a Step Function after midnight UTC, the crawler-emitted table name will differ from what the Glue job expects.
- `grafana-dashboard.json` has an unresolved git merge conflict sitting in the working tree (pre-existing, not from this bbox/commercial work) — resolve before editing it.
