from pyspark.sql import DataFrame, Window, functions as F

_EARTH_RADIUS_KM = 6371.0


def add_expected_price(
    df: DataFrame,
    *,
    price_col: str,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
    k: int = 15,
    min_rows: int = 30,
) -> DataFrame:
    """Estimate `expected_price` as the median `price_col` among each listing's `k`
    geographically nearest OTHER listings (great-circle distance on `lat_col`/`lon_col`)
    — pure geo comps, no district buckets, no room/type matching. Replaces an earlier
    regression-on-micro_district approach: administrative districts with only a
    handful of listings gave the regression ~0 training signal for that district in
    some cross-fit folds and produced wild predictions (e.g. $249/m² for an office
    where every comparable in the city was $1900-2500/m²). Distance-based neighbors
    degrade gracefully instead — an isolated listing just gets a wider/less confident
    comp radius, not nonsense.

    A listing's own price never contributes to its own `expected_price` (neighbors are
    always OTHER listings), so this needs no train/holdout split, unlike a fitted model.

    `price_vs_expected_pct` < 0 means priced below its geographic comps.

    Rows with a null lat/lon get a null `expected_price` (can't be located) and aren't
    used as candidate neighbors for anyone else.

    # ponytail: O(n^2) self cross-join to rank neighbors by distance — fine at Bishkek
    # listing volumes (low thousands per batch); switch to geohash-bucketed blocking if
    # a batch grows an order of magnitude and this gets slow.

    Skips (adds nulls) if there isn't enough data in the batch.
    """
    if df.count() < min_rows:
        return (
            df.withColumn("expected_price", F.lit(None).cast("double"))
              .withColumn("price_vs_expected_pct", F.lit(None).cast("double"))
        )

    has_coords = df.filter(F.col(lat_col).isNotNull() & F.col(lon_col).isNotNull())
    no_coords = df.filter(F.col(lat_col).isNull() | F.col(lon_col).isNull())

    a = has_coords.select("slug", lat_col, lon_col, price_col).alias("a")
    b = has_coords.select("slug", lat_col, lon_col, price_col).alias("b")

    lat1, lat2 = F.radians(F.col(f"a.{lat_col}")), F.radians(F.col(f"b.{lat_col}"))
    dlat = lat2 - lat1
    dlon = F.radians(F.col(f"b.{lon_col}") - F.col(f"a.{lon_col}"))
    hav = F.sin(dlat / 2) ** 2 + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2) ** 2
    dist_km = F.lit(2 * _EARTH_RADIUS_KM) * F.asin(F.sqrt(hav))

    pairs = a.crossJoin(b).where(F.col("a.slug") != F.col("b.slug")).withColumn("_dist", dist_km)

    ranked = pairs.withColumn(
        "_rank", F.row_number().over(Window.partitionBy(F.col("a.slug")).orderBy(F.col("_dist").asc()))
    ).filter(F.col("_rank") <= k)

    expected = ranked.groupBy(F.col("a.slug").alias("slug")).agg(
        F.expr(f"percentile_approx(b.{price_col}, 0.5)").alias("expected_price")
    )

    scored = has_coords.join(expected, "slug", "left").unionByName(
        no_coords.withColumn("expected_price", F.lit(None).cast("double"))
    )

    return scored.withColumn(
        "price_vs_expected_pct",
        (F.col(price_col) - F.col("expected_price")) / F.col("expected_price") * 100.0,
    )


def adjust_expected_price_for_flag(
    df: DataFrame,
    *,
    price_col: str,
    flag_col: str,
    min_group_size: int = 8,
) -> DataFrame:
    """Re-derive `expected_price`/`price_vs_expected_pct` for rows where `flag_col` is
    true, using a discount factor learned from THIS batch: the median ratio of
    `price_col` to the existing (flag-blind) `expected_price` among all flag_col=true
    rows. Call after `add_expected_price`.

    Fixes a systematic bias `add_expected_price` can't see on its own: a flagged
    row's geographic neighbors are mostly NOT flagged (e.g. a basement unit's nearest
    neighbors are mostly regular floors), so its kNN expected_price ignores whatever
    made it legitimately cheaper (or pricier) — it looks "underpriced" purely from
    the category effect, not because it's actually a good deal.

    Skips (leaves expected_price/price_vs_expected_pct untouched) below
    `min_group_size` flagged rows with a non-null expected_price in this batch — same
    reasoning as the min_group_size this replaced for districts: too few examples to
    estimate a stable factor, and a wrong guessed factor is worse than none.
    """
    flagged = df.filter(F.col(flag_col) & F.col("expected_price").isNotNull())
    if flagged.count() < min_group_size:
        return df

    factor = flagged.select(
        F.expr(f"percentile_approx({price_col} / expected_price, 0.5)").alias("factor")
    ).first()["factor"]

    adjusted_expected = F.when(
        F.col(flag_col), F.col("expected_price") * F.lit(factor)
    ).otherwise(F.col("expected_price"))

    return (
        df.withColumn("expected_price", adjusted_expected)
        .withColumn(
            "price_vs_expected_pct",
            (F.col(price_col) - F.col("expected_price")) / F.col("expected_price") * 100.0,
        )
    )


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
