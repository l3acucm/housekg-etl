# Commercial rent yield — design

Date: 2026-08-18

## Purpose

The commercial dashboard currently shows only sale listings. The user wants to
estimate rental yield / payback period for each sale listing, using nearby
*rental* commercial listings as comps, and see it interactively on the map:
click a sale object → map narrows to that object plus the 3 rental comps used
for its estimate, plus a stat panel with the payback/yield numbers → a
dashboard link resets back to the full filtered view.

## Decisions locked in during brainstorming

- Yield formula: `avg(rent price/m² of 3 nearest rental listings) × sale
  object's own square footage` — not a raw average of comps' absolute rent
  (normalizes for size differences between the sale object and its comps).
- Comp selection: pure geographic 3-nearest, **no** `commercial_type` filter —
  consistent with `price_model.add_expected_price`'s existing philosophy
  (that function deliberately dropped district/type bucketing after a bug
  where sparse buckets produced wild predictions; same reasoning applies to
  filtering by type here — risks starving a comp search near a rare type).
- Ingestion architecture: rental ingestion is folded **sequentially into the
  existing commercial Step Function branch**, not a new parallel branch — the
  `Parallel` state has no per-branch `Catch` (documented gotcha in
  `CLAUDE.md`), so a 4th independent branch would add a new failure surface;
  rent data is only ever consumed by the commercial job anyway.
- Click mechanism: a second Grafana data link on the sale marker's `slug`
  field, navigating to the same dashboard with `?var-selected_slug=<slug>` —
  same mechanism already used for the existing "View Details" → house.kg
  link. This is a page navigation/reload with a new variable value, not
  in-place JS filtering (no plugin/custom panel work).
- Reset: a top-level dashboard link "Показать все объекты" →
  `?var-selected_slug=` (clears the variable, panels revert to the full
  filtered view).
- Displayed metrics: **both** `payback_months` and `monthly_yield_pct` (same
  two numbers, shown two ways) plus `est_monthly_rent`.
- Map color for sale markers switches from `price_vs_expected_pct` (existing
  scheme) to `payback_months`, buckets in **years**, 6 colors:

  | Payback (years) | Months (derived) | Color |
  |---|---|---|
  | ≤ 5 | ≤ 60 | `dark-green` |
  | 6 | 61–72 | `green` |
  | 7 | 73–84 | `yellow` |
  | 8 | 85–96 | `orange` |
  | 9 | 97–108 | `red` |
  | ≥ 10 | ≥ 109 | `dark-red` |

  Grafana threshold steps (ascending, ignoring the first value per Grafana's
  own semantics): `[{color: dark-green, value: 0}, {color: green, value: 61},
  {color: yellow, value: 73}, {color: orange, value: 85}, {color: red, value:
  97}, {color: dark-red, value: 109}]`.

## 1. Ingestion — new Lambda, folded into the commercial branch

New file `etl/jobs/lambda/commercial_rent_ingestion/main.py`: clone of
`commercial_ingestion/main.py` with two changes — `filter.type_id.value` from
`["1"]` to `["2"]`, and the bbox replaced with the unified bbox already used
by all three existing lambdas (`lat1=42.529879066020332&lon1=74.01283264160158&lat2=43.096546175778314&lon2=75.08399963378908`)
— **not** the wide bbox pasted in the request, which was from an ad-hoc
manual query. S3 key prefix: `ingestions_commercial_rent/`.

`iac/lambda.tf`: new `aws_lambda_function.commercial_rent_ingestion_lambda`
(same shape as `commercial_ingestion_lambda`: 512MB/60s, `requests_layer`),
env `FILE_NAME_PREFIX="commercial_rent"`,
`CRAWLER_NAME="commercial_rent_ingestions_crawler"`. Add the new S3 prefix to
`lambda_role_policy`'s `s3:PutObject` resource list.

`iac/glue.tf`: new `aws_glue_crawler.commercial_rent_ingestions_crawler`
targeting `s3://.../ingestions_commercial_rent/`, same classifier/config as
`commercial_ingestions_crawler`.

`etl/jobs/pack.sh`: add the new lambda to the zip step.

`iac/sf.tf`, commercial branch only: insert between the existing
`CommercialCrawlerStatusChoice` (default case) and `StartCommercialGlueJob`:

```
CommercialCrawlerStatusChoice --(Default)--> IngestCommercialRent
  --> RunCommercialRentIngestionCrawler --> WaitCommercialRentCrawler
  --> CheckCommercialRentCrawler --> CommercialRentCrawlerStatusChoice
  --(Default)--> StartCommercialGlueJob
```
(mirrors the existing ingest→crawl→check loop pattern exactly). Extend
`step_function_role`'s `lambda:InvokeFunction` resource list with the new
lambda's ARN.

## 2. Yield computation — Glue job + shared module

`etl/jobs/glue/commercial_feature_engineering.py`:
- New `get_bronze_rent_df()` mirrors `get_bronze_df()` but reads
  `commercial_rent_<DDMMYYYY>_json` and selects only `slug`, `latitude`,
  `longitude`, `sqm_price` (rent/m²/month, same `prices[1].m2_price` field as
  sale — **unverified assumption**, see Risks), `square`. Filters: positive
  coords, positive `sqm_price`, positive `square`. No hardcoded price band
  yet (YAGNI — add one once a real batch shows what "plausible" rent/m²
  looks like).
- Call `price_model.nearest_cross_comps(scored_df, rent_df, k=3, ...)` after
  the existing `adjust_expected_price_for_flag` call.

`etl/jobs/glue/price_model.py`: new `nearest_cross_comps(sale_df, comp_df, *,
k=3, price_col, lat_col="latitude", lon_col="longitude")`. Same haversine
`crossJoin` + `row_number` window pattern as `add_expected_price`, but
**cross-dataset** (comps come from `comp_df`, not `sale_df` itself — no
self-exclusion needed). For each sale row, returns up to `k` flattened comp
columns (`comp1_slug/lat/lon/rent_sqm_price`, `comp2_*`, `comp3_*` — null if
fewer than `k` comps exist in the whole batch) plus:
- `avg_rent_sqm_price` = mean of the found comps' `sqm_price`
- `est_monthly_rent` = `avg_rent_sqm_price × square`
- `payback_months` = `price_usd / est_monthly_rent`
- `monthly_yield_pct` = `est_monthly_rent / price_usd × 100`

All four null if `comp_df` is empty for the batch (no min-row guard beyond
that — 3 nearest of *whatever* exists, same "degrades gracefully" philosophy
as `add_expected_price`).

## 3. Storage

New columns land in `commercial_price_fact` (SCD2), alongside the existing
`expected_price`/`price_vs_expected_pct` — same table, same job, same
mechanism, so they get `_change` tracking for the double-typed fields for
free: `payback_months`, `monthly_yield_pct`, `est_monthly_rent` (all
`DoubleType`), plus the 12 flat comp fields (`comp{1,2,3}_slug/lat/lon/rent_sqm_price`
— not change-tracked, just carried through). Reminder of the existing
schema-propagation gotcha: after this ships, `commercial_price_fact` crawler
must run once after the job writes the new columns (`terraform apply` → run
job → run crawler), or Grafana/Athena will 404 on the new columns.

## 4. Grafana — `grafana-dashboard-commercial.json`

New variable `selected_slug` (Textbox, default `""`).

Map panel (`panel-10`), layer **Listings** (existing sale layer):
- Query gets `AND (${selected_slug} = '' OR c.slug = '${selected_slug}')`
  appended.
- Color: replace the `price_vs_expected_pct` threshold config with the
  `payback_months` 6-step scheme above.
- `slug` field gets a second data link (existing "View Details" stays):
  `title: "Show yield & comps"`, `url: "?var-selected_slug=${__value.raw}"`.

New layer **Rental comps** (same panel, new `refId`): query unions the three
comp columns into rows (`SELECT comp1_lat AS latitude, comp1_slug AS slug,
comp1_rent_sqm_price AS price ... UNION ALL comp2... UNION ALL comp3...
FROM commercial_price_fact WHERE slug = '${selected_slug}' AND is_current =
true`), returns 0 rows when `selected_slug` is empty. Distinct marker
style/color from the sale layer so the two are visually separable.

New Stat panel "Доходность": `SELECT payback_months, monthly_yield_pct,
est_monthly_rent FROM commercial_price_fact WHERE slug = '${selected_slug}'
AND is_current = true`. Shows "No data" when nothing is selected — acceptable,
no extra show/hide logic needed.

Top-level dashboard `links`: new entry "Показать все объекты" →
`?var-selected_slug=`.

## 5. Testing / verification

- Arithmetic and kNN pattern reuse `add_expected_price`'s already-proven
  haversine logic 1:1 (just cross-dataset instead of self-join) — no new
  algorithmic risk.
- No pyspark available in this sandbox (same limitation hit for
  `anomaly_correction.py` earlier this session) — can't unit-test the Spark
  logic locally. Verify via `terraform apply` + `aws glue start-job-run`
  (manual run, same as this repo's existing setup workflow) against a real
  batch once rent ingestion has landed at least one day of data.
- Grafana click/reset behavior can't be tested from this sandbox either (no
  live Grafana instance here) — needs manual click-through after the
  dashboard JSON is deployed.

## Risks / open items

1. Unverified: whether `prices[1].m2_price` on a *rental* house.kg listing
   means "rent per m² per month" (assumed) vs. something else. Confirm once
   real ingestion data lands; if wrong, `get_bronze_rent_df()`'s price
   extraction needs adjusting.
2. No price-band filter on rent data yet — first real batch may need one
   (mirrors how sale price bands were tuned from real p1–p99 distributions
   per kind, documented in `CLAUDE.md`).
3. `commercial_price_fact`'s schema will grow by 15 columns — after deploy,
   don't forget the crawler re-run step (existing documented gotcha).
