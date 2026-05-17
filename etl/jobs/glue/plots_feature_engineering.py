import sys
from typing import List
from pyspark.sql import functions as F, types as T, DataFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from datetime import datetime
import boto3

from anomaly_correction import flag_anomalies, correct_anomalies_with_haiku

glue_context = GlueContext(SparkContext())
args = getResolvedOptions(sys.argv, ['BUCKET', 'MODEL_ID', 'BEDROCK_REGION', 'MAX_LLM_CALLS'])
bucket = args['BUCKET']
model_id = args['MODEL_ID']
bedrock_region = args['BEDROCK_REGION']
max_llm_calls = int(args['MAX_LLM_CALLS'])
logger = glue_context.get_logger()
spark = glue_context.spark_session

silver_key = "silver"
glue_database_name = "realty_data"
glue_client = boto3.client('glue')
timestamp_str = datetime.now().strftime("%d%m%Y")

price_fact_table_name = "plots_price_fact"
plots_dim_table_name = "plots_dim"
market_summary_table_name = "plots_market_summary"
plots_dim_table_s3_uri = f"s3://{bucket}/{silver_key}/{plots_dim_table_name}"
price_fact_s3_uri = f"s3://{bucket}/{silver_key}/{price_fact_table_name}"
market_summary_s3_uri = f"s3://{bucket}/{silver_key}/{market_summary_table_name}"

bronze_table_name = f"plots_{timestamp_str}_json"


def get_bronze_df():
    df_bronze = glue_context.create_dynamic_frame.from_catalog(
        database=glue_database_name,
        table_name=bronze_table_name
    ).toDF().select(F.explode(F.col("list")).alias("plot")).select('plot.*')

    land_square_double = F.coalesce(
        F.col("land_square.double"),
        F.col("land_square.int").cast(T.DoubleType())
    )
    price_usd_double = F.coalesce(
        F.col("prices")[1]["price"]["int"].cast(T.DoubleType()),
        F.col("prices")[1]["price"]["long"].cast(T.DoubleType()),
    )

    has_construction = F.array_contains(F.col("document"), 6)
    has_sown = F.array_contains(F.col("document"), 7)

    return (df_bronze.select(
                F.col('slug'),
                F.col('longitude'),
                F.col('latitude'),
                F.col('prices')[1]['are_price'].cast(T.DoubleType()).alias('are_price'),
                price_usd_double.alias('price_usd'),
                land_square_double.alias('land_square'),
                F.col('district'),
                F.col('micro_district'),
                F.col('document'),
                F.col('land_location'),
                F.col('description'),
                F.col('updated_at'),
            )
            .withColumn("updated_at", F.current_timestamp())
            .withColumn(
                "purpose",
                F.when(has_construction & ~has_sown, F.lit("construction"))
                 .when(has_sown & ~has_construction, F.lit("sown"))
            )
            .dropDuplicates(["slug"])
            .filter(F.col("are_price") > 0)
            .filter(F.col("land_square") > 0)
            .filter(F.col("micro_district").isNotNull())
            .filter(F.col("purpose").isNotNull())
            )


def apply_anomaly_corrections(df: DataFrame) -> DataFrame:
    """Flag rows with implausible are_price (vs hard prior and log-MAD by district+purpose),
    ask Haiku to extract the real area/price from the seller's description, and recompute
    are_price from the corrected values. Adds audit columns and drops `description`."""
    flagged = flag_anomalies(
        df,
        value_col="are_price",
        group_cols=["district", "purpose"],
        hard_low=50.0,
        hard_high=80000.0,
        min_group_size=8,
        mad_k=3.0,
    )

    anomaly_rows = (
        flagged.filter(F.col("is_anomaly"))
        .select("slug", "description", "land_square", "price_usd", "are_price")
        .collect()
    )

    corrections = {}
    if anomaly_rows:
        logger.info(f"Plots anomalies flagged: {len(anomaly_rows)}; invoking Bedrock (cap={max_llm_calls})")
        corrections = correct_anomalies_with_haiku(
            [
                {
                    "slug": r["slug"],
                    "description": r["description"],
                    "structured_square": r["land_square"],
                    "structured_price_usd": r["price_usd"],
                    "implied_per_unit": r["are_price"],
                }
                for r in anomaly_rows
            ],
            kind="land plot",
            unit_label="are",
            model_id=model_id,
            region=bedrock_region,
            max_calls=max_llm_calls,
        )

    corr_schema = T.StructType([
        T.StructField("slug", T.StringType(), False),
        T.StructField("corr_land_square", T.DoubleType(), True),
        T.StructField("corr_price_usd", T.DoubleType(), True),
        T.StructField("llm_confidence", T.StringType(), True),
    ])
    corr_rows = []
    for slug, parsed in corrections.items():
        sq = parsed.get("actual_square_are")
        pr = parsed.get("actual_price_usd")
        corr_rows.append((
            slug,
            float(sq) if isinstance(sq, (int, float)) and sq > 0 else None,
            float(pr) if isinstance(pr, (int, float)) and pr > 0 else None,
            parsed.get("confidence"),
        ))
    corr_df = spark.createDataFrame(corr_rows, corr_schema)

    return (
        flagged.join(corr_df, ["slug"], "left")
        .withColumn("square_original", F.col("land_square"))
        .withColumn("price_original", F.col("price_usd"))
        .withColumn("square_corrected_by_llm", F.col("corr_land_square").isNotNull())
        .withColumn("price_corrected_by_llm", F.col("corr_price_usd").isNotNull())
        .withColumn("land_square", F.coalesce(F.col("corr_land_square"), F.col("land_square")))
        .withColumn("price_usd", F.coalesce(F.col("corr_price_usd"), F.col("price_usd")))
        .withColumn(
            "are_price",
            F.when(F.col("land_square") > 0, F.col("price_usd") / F.col("land_square"))
            .otherwise(F.col("are_price")),
        )
        .drop("corr_land_square", "corr_price_usd", "is_anomaly", "description")
    )


def apply_strict_filters(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("purpose") == "sown")
        | ((F.col("are_price") > 3000) & (F.col("are_price") < 100000))
    )


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


def create_plots_market_summary_table(cleaned_df_bronze):
    df_with_price = cleaned_df_bronze.withColumn("total_price", F.col("are_price") * F.col("land_square"))

    district_summary = df_with_price.groupBy("micro_district", "purpose").agg(
        F.count("slug").cast(T.DoubleType()).alias("object_count"),
        F.sum("total_price").alias("total_price"),
        F.sum("land_square").alias("total_land_square")
    )

    market_summary = df_with_price.groupBy("purpose").agg(
        F.lit(None).cast(T.StringType()).alias("micro_district"),
        F.count("slug").cast(T.DoubleType()).alias("object_count"),
        F.sum("total_price").alias("total_price"),
        F.sum("land_square").alias("total_land_square")
    ).select("micro_district", "purpose", "object_count", "total_price", "total_land_square")

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
        T.StructField("total_land_square", T.DoubleType(), False)
    ]

    comparison_df = combined_summary.select(
        "slug",
        "purpose",
        "object_count",
        "total_price",
        "total_land_square",
        F.current_timestamp().alias("timestamp")
    )

    update_scd2_table(
        key_fields=[
            T.StructField("slug", T.StringType(), False),
            T.StructField("purpose", T.StringType(), False),
        ],
        parquet_path=market_summary_s3_uri,
        compared_fields=fields,
        comparison_df=comparison_df,
        partition_col="is_current"
    )


def main():
    bronze_df = get_bronze_df().alias("incoming")
    bronze_df.cache()

    corrected_df = apply_anomaly_corrections(bronze_df)
    cleaned_df_bronze = apply_strict_filters(corrected_df)
    cleaned_df_bronze.cache()

    plots_dim_df = cleaned_df_bronze.drop("are_price")
    plots_dim_df.write.mode("overwrite").parquet(plots_dim_table_s3_uri)

    update_scd2_table(
        key_fields=[T.StructField("slug", T.StringType(), False)],
        parquet_path=price_fact_s3_uri,
        compared_fields=[T.StructField("are_price", T.DoubleType(), False)],
        comparison_df=cleaned_df_bronze.select("slug", "are_price", F.col("updated_at").alias("timestamp")),
        partition_col="is_current"
    )

    create_plots_market_summary_table(cleaned_df_bronze)


if __name__ == "__main__":
    main()
