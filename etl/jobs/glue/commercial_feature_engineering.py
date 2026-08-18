import sys
from typing import List
from pyspark.sql import functions as F, types as T, DataFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from datetime import datetime
import boto3

from anomaly_correction import (
    flag_anomalies,
    correct_anomalies_with_haiku,
    flag_keyword_candidates,
    classify_with_haiku,
    LEASE_RIGHTS_PATTERN,
    LEASE_RIGHTS_SYSTEM_PROMPT,
    LEASE_RIGHTS_QUESTION_KEY,
    BASEMENT_PATTERN,
    BASEMENT_SYSTEM_PROMPT,
    BASEMENT_QUESTION_KEY,
)
from notify import find_new_rows, send_webhook
from price_model import add_expected_price, adjust_expected_price_for_flag, nearest_cross_comps

glue_context = GlueContext(SparkContext())
args = getResolvedOptions(sys.argv, ['BUCKET', 'MODEL_ID', 'BEDROCK_REGION', 'MAX_LLM_CALLS', 'WEBHOOK_URL'])
bucket = args['BUCKET']
model_id = args['MODEL_ID']
bedrock_region = args['BEDROCK_REGION']
max_llm_calls = int(args['MAX_LLM_CALLS'])
webhook_url = args['WEBHOOK_URL']
logger = glue_context.get_logger()
spark = glue_context.spark_session

# Keyword-flagged candidates (lease-rights, basement) are cheap yes/no Haiku calls,
# not the more involved anomaly correction — cap separately and higher; a batch can
# easily have 100+ genuine "цоколь" mentions and MAX_LLM_CALLS is tuned for the
# anomaly-correction path.
_KEYWORD_CLASSIFY_MAX_CALLS = 300

silver_key = "silver"
glue_database_name = "realty_data"
glue_client = boto3.client('glue')
timestamp_str = datetime.now().strftime("%d%m%Y")

price_fact_table_name = "commercial_price_fact"
commercial_dim_table_name = "commercial_dim"
market_summary_table_name = "commercial_market_summary"
commercial_dim_table_s3_uri = f"s3://{bucket}/{silver_key}/{commercial_dim_table_name}"
price_fact_s3_uri = f"s3://{bucket}/{silver_key}/{price_fact_table_name}"
market_summary_s3_uri = f"s3://{bucket}/{silver_key}/{market_summary_table_name}"

bronze_table_name = f"commercial_{timestamp_str}_json"
bronze_rent_table_name = f"commercial_rent_{timestamp_str}_json"

# id -> label from house.kg's "тип коммерческого помещения" dropdown (site UI order, 1-indexed)
COMMERCIAL_TYPE_LABELS = {
    1: "магазины, бутики",
    2: "офисы",
    3: "торговые контейнеры",
    4: "сельское хозяйство",
    5: "рестораны, кафе, общепит",
    6: "отели, хостелы, гостиницы, зоны отдыха",
    7: "цеха, заводы, фабрики, мастерские",
    8: "автосервисы, автомойки, автобизнес",
    9: "салоны красоты",
    10: "медцентры, аптеки",
    11: "здания",
    12: "склады",
    13: "другая коммерческая недвижимость",
}


def get_bronze_df():
    df_bronze = glue_context.create_dynamic_frame.from_catalog(
        database=glue_database_name,
        table_name=bronze_table_name
    ).toDF().select(F.explode(F.col("list")).alias("house")).select('house.*')

    square_double = F.coalesce(
        F.col("square.double"),
        F.col("square.int").cast(T.DoubleType())
    )
    land_square_double = F.coalesce(
        F.col("land_square.double"),
        F.col("land_square.int").cast(T.DoubleType())
    )
    price_usd_double = F.coalesce(
        F.col("prices")[1]["price"]["double"].cast(T.DoubleType()),
        F.col("prices")[1]["price"]["int"].cast(T.DoubleType()),
    )

    type_label = F.coalesce(
        F.create_map([F.lit(x) for pair in COMMERCIAL_TYPE_LABELS.items() for x in pair])[
            F.col("commercial_type")
        ],
        F.lit("не указано"),
    )

    df_bronze = df_bronze.select(
        F.col('slug'),
        F.col('longitude'),
        F.col('latitude'),
        F.col('prices')[1]['m2_price'].cast(T.DoubleType()).alias('sqm_price'),
        price_usd_double.alias('price_usd'),
        square_double.alias('square'),
        land_square_double.alias('land_square'),
        type_label.alias('commercial_type'),
        F.col('floor'),
        F.col('floors'),
        F.col('district'),
        F.col('micro_district'),
        F.col('description'),
        F.col('updated_at'),
        F.col('year'),
        F.col('condition'),
    )

    return (
        df_bronze
        .withColumn("updated_at", F.current_timestamp())
        .dropDuplicates(["slug"])
        .filter(F.col("sqm_price") > 0)
        .filter(F.col("square") > 0)
        .filter(F.col("micro_district").isNotNull())
    )


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


def apply_anomaly_corrections(df: DataFrame) -> DataFrame:
    flagged = flag_anomalies(
        df,
        value_col="sqm_price",
        group_cols=["district", "commercial_type"],
        hard_low=50.0,
        hard_high=15000.0,
        min_group_size=8,
        mad_k=3.0,
    )

    anomaly_rows = (
        flagged.filter(F.col("is_anomaly"))
        .select("slug", "description", "square", "price_usd", "sqm_price")
        .collect()
    )

    corrections = {}
    if anomaly_rows:
        logger.info(f"Commercial anomalies flagged: {len(anomaly_rows)}; invoking Bedrock (cap={max_llm_calls})")
        corrections = correct_anomalies_with_haiku(
            [
                {
                    "slug": r["slug"],
                    "description": r["description"],
                    "structured_square": r["square"],
                    "structured_price_usd": r["price_usd"],
                    "implied_per_unit": r["sqm_price"],
                }
                for r in anomaly_rows
            ],
            kind="commercial property",
            unit_label="m2",
            model_id=model_id,
            region=bedrock_region,
            max_calls=max_llm_calls,
        )

    corr_schema = T.StructType([
        T.StructField("slug", T.StringType(), False),
        T.StructField("corr_square", T.DoubleType(), True),
        T.StructField("corr_price_usd", T.DoubleType(), True),
        T.StructField("llm_confidence", T.StringType(), True),
    ])
    corr_rows = []
    for slug, parsed in corrections.items():
        sq = parsed.get("actual_square_m2")
        pr = parsed.get("actual_price_usd")
        corr_rows.append((
            slug,
            float(sq) if isinstance(sq, (int, float)) and sq > 0 else None,
            float(pr) if isinstance(pr, (int, float)) and pr > 0 else None,
            parsed.get("confidence"),
        ))
    corr_df = spark.createDataFrame(corr_rows, corr_schema)

    corrected = (
        flagged.join(corr_df, ["slug"], "left")
        .withColumn("square_original", F.col("square"))
        .withColumn("price_original", F.col("price_usd"))
        .withColumn("square_corrected_by_llm", F.col("corr_square").isNotNull())
        .withColumn("price_corrected_by_llm", F.col("corr_price_usd").isNotNull())
        .withColumn("square", F.coalesce(F.col("corr_square"), F.col("square")))
        .withColumn("price_usd", F.coalesce(F.col("corr_price_usd"), F.col("price_usd")))
        .withColumn(
            "sqm_price",
            F.when(F.col("square") > 0, F.col("price_usd") / F.col("square"))
            .otherwise(F.col("sqm_price")),
        )
        .drop("corr_square", "corr_price_usd", "is_anomaly", "price_usd")
    )

    corrected = filter_lease_right_sales(corrected)
    corrected = flag_basements(corrected)

    return corrected.drop("description")


def filter_lease_right_sales(df: DataFrame) -> DataFrame:
    """Drop rows where the description confirms this is a lease-right sale (переуступка
    прав аренды), not a sale of the property itself — pollutes price comps if left in,
    and price-based anomaly detection can't catch it (see module docstring in
    anomaly_correction.py)."""
    flagged = flag_keyword_candidates(df, pattern=LEASE_RIGHTS_PATTERN, out_col="_lease_candidate")
    candidate_rows = flagged.filter(F.col("_lease_candidate")).select("slug", "description").collect()

    confirmed = set()
    if candidate_rows:
        logger.info(f"Lease-right candidates flagged: {len(candidate_rows)}; invoking Bedrock (cap={_KEYWORD_CLASSIFY_MAX_CALLS})")
        confirmed = classify_with_haiku(
            [{"slug": r["slug"], "description": r["description"]} for r in candidate_rows],
            system_prompt=LEASE_RIGHTS_SYSTEM_PROMPT,
            question_key=LEASE_RIGHTS_QUESTION_KEY,
            model_id=model_id,
            region=bedrock_region,
            max_calls=_KEYWORD_CLASSIFY_MAX_CALLS,
        )

    kept = flagged.filter(~F.col("slug").isin(list(confirmed))) if confirmed else flagged
    return kept.drop("_lease_candidate")


def flag_basements(df: DataFrame) -> DataFrame:
    """Adds boolean `is_basement` (цоколь/полуподвал) from the description — the
    structured `floor` field is often null and never distinguishes a basement level
    from a normal one anyway. `price_model.adjust_expected_price_for_flag` uses this
    to correct for the floor-level discount kNN's geographic neighbors can't see."""
    flagged = flag_keyword_candidates(df, pattern=BASEMENT_PATTERN, out_col="_basement_candidate")
    candidate_rows = flagged.filter(F.col("_basement_candidate")).select("slug", "description").collect()

    confirmed = set()
    if candidate_rows:
        logger.info(f"Basement candidates flagged: {len(candidate_rows)}; invoking Bedrock (cap={_KEYWORD_CLASSIFY_MAX_CALLS})")
        confirmed = classify_with_haiku(
            [{"slug": r["slug"], "description": r["description"]} for r in candidate_rows],
            system_prompt=BASEMENT_SYSTEM_PROMPT,
            question_key=BASEMENT_QUESTION_KEY,
            model_id=model_id,
            region=bedrock_region,
            max_calls=_KEYWORD_CLASSIFY_MAX_CALLS,
        )

    confirmed_list = list(confirmed)
    is_basement = F.col("slug").isin(confirmed_list) if confirmed_list else F.lit(False)
    return flagged.withColumn("is_basement", is_basement).drop("_basement_candidate")


def apply_strict_filters(df: DataFrame) -> DataFrame:
    return df.filter(F.col("sqm_price") > 100).filter(F.col("sqm_price") < 8000)


def update_scd2_table(*, key_fields: List[T.StructField], parquet_path: str, compared_fields: List[T.StructField],
                      comparison_df: DataFrame, partition_col: str):
    schema_fields = list(key_fields)

    for cf in compared_fields:
        schema_fields.append(cf)
        if isinstance(cf.dataType, T.DoubleType):
            schema_fields.append(T.StructField(f"{cf.name}_change", T.DoubleType(), True))

    schema_fields.extend([
        T.StructField("effective_from", T.TimestampType(), False),
        T.StructField("effective_to", T.TimestampType(), True),
        T.StructField("is_current", T.BooleanType(), False)
    ])

    schema = T.StructType(schema_fields)
    try:
        current_scd2_df = spark.read.schema(schema).parquet(parquet_path)
    except:
        current_scd2_df = spark.createDataFrame([], schema)
    current_scd2_df.cache()

    join_cond = None
    for kf in key_fields:
        cond = comparison_df[kf.name] == current_scd2_df[kf.name]
        join_cond = cond if join_cond is None else (join_cond & cond)

    joined_df = comparison_df.alias("new").join(
        current_scd2_df.filter("is_current = true").alias("current"),
        join_cond,
        "full_outer"
    )

    fields_comparison_list = []
    for cf in compared_fields:
        fields_comparison_list.append(
            f"(new.{cf.name} IS NOT NULL AND current.{cf.name} IS NOT NULL AND new.{cf.name} != current.{cf.name}) OR " +
            f"(new.{cf.name} IS NULL AND current.{cf.name} IS NOT NULL) OR " +
            f"(new.{cf.name} IS NOT NULL AND current.{cf.name} IS NULL)"
        )
    fields_comparison_filter_str = ' OR '.join(fields_comparison_list)

    keys_not_null_current = ' AND '.join([f"current.{kf.name} IS NOT NULL" for kf in key_fields])
    keys_not_null_new = ' AND '.join([f"new.{kf.name} IS NOT NULL" for kf in key_fields])
    keys_null_current = ' AND '.join([f"current.{kf.name} IS NULL" for kf in key_fields])

    updates_select = [F.col(f"current.{kf.name}") for kf in key_fields]
    for cf in compared_fields:
        updates_select.append(F.col(f"current.{cf.name}"))
        if isinstance(cf.dataType, T.DoubleType):
            change_field_name = f"{cf.name}_change"
            updates_select.append(F.col(f"current.{change_field_name}"))
    updates_select.extend([
        F.col("current.effective_from"),
        F.current_timestamp().alias("effective_to"),
        F.lit(False).alias("is_current")
    ])

    updates_df = joined_df.where(
        f"{keys_not_null_current} AND {keys_not_null_new} AND ({fields_comparison_filter_str})"
    ).select(*updates_select)

    new_records_select = [
        F.coalesce(F.col(f"new.{kf.name}"), F.col(f"current.{kf.name}")).alias(kf.name)
        for kf in key_fields
    ]
    for cf in compared_fields:
        new_records_select.append(F.col(f"new.{cf.name}"))
        if isinstance(cf.dataType, T.DoubleType):
            change_field_name = f"{cf.name}_change"
            new_records_select.append(
                F.when(
                    (F.col(f"current.{cf.name}").isNull()) |
                    (F.col(f"current.{cf.name}") == 0) |
                    (F.col(f"new.{cf.name}").isNull()),
                    F.lit(0)
                ).otherwise(
                    ((F.col(f"new.{cf.name}") - F.col(f"current.{cf.name}")) /
                     F.col(f"current.{cf.name}") * 100.0)
                ).alias(change_field_name)
            )
    new_records_select.extend([
        F.current_timestamp().alias("effective_from"),
        F.lit(None).cast("timestamp").alias("effective_to"),
        F.lit(True).alias("is_current")
    ])

    new_records_df = joined_df.where(
        f"({keys_null_current}) OR ({keys_not_null_current} AND ({fields_comparison_filter_str}))"
    ).select(*new_records_select)

    unchanged_records_select = [F.col(f"current.{kf.name}") for kf in key_fields]
    for cf in compared_fields:
        unchanged_records_select.append(F.col(f"current.{cf.name}").cast(cf.dataType))
        if isinstance(cf.dataType, T.DoubleType):
            change_field_name = f"{cf.name}_change"
            unchanged_records_select.append(F.col(f"current.{change_field_name}"))
    unchanged_records_select.extend([
        F.col("current.effective_from"),
        F.col("current.effective_to"),
        F.col("current.is_current")
    ])

    unchanged_records_df = joined_df.where(
        f"{keys_not_null_current} AND {keys_not_null_new} AND NOT ({fields_comparison_filter_str})"
    ).select(*unchanged_records_select)

    historical_records_df = current_scd2_df.filter("is_current = false")

    to_close_select = [F.col(kf.name) for kf in key_fields]
    for cf in compared_fields:
        to_close_select.append(F.col(cf.name))
        if isinstance(cf.dataType, T.DoubleType):
            change_field_name = f"{cf.name}_change"
            to_close_select.append(F.col(change_field_name))
    to_close_select.extend([
        F.col("effective_from"),
        F.current_timestamp().alias("effective_to"),
        F.lit(False).alias("is_current")
    ])

    anti_join_cond = None
    for kf in key_fields:
        cond = F.col(f"current.{kf.name}") == F.col(f"new.{kf.name}")
        anti_join_cond = cond if anti_join_cond is None else (anti_join_cond & cond)

    to_close_df = current_scd2_df.filter("is_current = true").alias("current").join(
        comparison_df.alias("new"),
        anti_join_cond,
        "left_anti"
    ).select(*to_close_select)

    new_df = updates_df.unionAll(new_records_df).unionAll(unchanged_records_df).unionAll(
        historical_records_df).unionAll(to_close_df)
    new_df.write.mode("overwrite").parquet(parquet_path)
    new_df.unpersist()
    return spark.read.parquet(parquet_path)


def create_commercial_market_summary_table(cleaned_df_bronze):
    df_with_price = cleaned_df_bronze.withColumn("total_price", F.col("sqm_price") * F.col("square"))

    district_summary = df_with_price.groupBy("micro_district", "commercial_type").agg(
        F.count("slug").cast(T.DoubleType()).alias("object_count"),
        F.sum("total_price").alias("total_price"),
        F.sum("square").alias("total_square")
    )

    market_summary = df_with_price.groupBy("commercial_type").agg(
        F.lit(None).cast(T.StringType()).alias("micro_district"),
        F.count("slug").cast(T.DoubleType()).alias("object_count"),
        F.sum("total_price").alias("total_price"),
        F.sum("square").alias("total_square")
    ).select("micro_district", "commercial_type", "object_count", "total_price", "total_square")

    combined_summary = district_summary.unionByName(market_summary)

    combined_summary = combined_summary.withColumn(
        "slug",
        F.when(
            F.col("micro_district").isNull(),
            F.lit("market")
        ).otherwise(
            F.col("micro_district")
        )
    ).drop('micro_district')

    fields = [
        T.StructField("object_count", T.DoubleType(), False),
        T.StructField("total_price", T.DoubleType(), False),
        T.StructField("total_square", T.DoubleType(), False)
    ]

    comparison_df = combined_summary.select(
        "slug",
        "commercial_type",
        "object_count",
        "total_price",
        "total_square",
        F.current_timestamp().alias("timestamp")
    )

    update_scd2_table(
        key_fields=[
            T.StructField("slug", T.StringType(), False),
            T.StructField("commercial_type", T.StringType(), False),
        ],
        parquet_path=market_summary_s3_uri,
        compared_fields=fields,
        comparison_df=comparison_df,
        partition_col="is_current"
    )


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
