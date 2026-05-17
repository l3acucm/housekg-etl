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

s3_client = boto3.client('s3')
price_fact_table_name = f"realty_price_fact"
realty_dim_table_name = f"realty_dim"
market_summary_table_name = f"market_summary"
realty_dim_table_s3_uri = f"s3://{bucket}/{silver_key}/{realty_dim_table_name}"
price_fact_s3_uri = f"s3://{bucket}/{silver_key}/{price_fact_table_name}"
market_summary_s3_uri = f"s3://{bucket}/{silver_key}/{market_summary_table_name}"

bronze_table_name = f"apartments_{timestamp_str}_json"


def get_bronze_df():
    df_bronze = glue_context.create_dynamic_frame.from_catalog(
        database=glue_database_name,
        table_name=bronze_table_name
    ).toDF().select(F.explode(F.col("list")).alias("house")).select('house.*')

    square_double = F.coalesce(
        F.col("square.double"),
        F.col("square.int").cast(T.DoubleType())
    )
    price_usd_double = F.coalesce(
        F.col("prices")[1]["price"]["double"].cast(T.DoubleType()),
        F.col("prices")[1]["price"]["int"].cast(T.DoubleType()),
    )

    df_bronze = df_bronze.select(
        F.col('slug'),
        F.col('longitude'),
        F.col('latitude'),
        F.col('prices')[1]['m2_price'].cast(T.DoubleType()).alias('sqm_price'),
        price_usd_double.alias('price_usd'),
        square_double.alias('square'),
        F.col('kitchen_square'),
        F.col('floor'),
        F.col('floors'),
        F.col('district'),
        F.col('micro_district'),
        F.col('description'),
        F.col('updated_at'),
        F.col('year'),
        F.col('toilet'),
        F.col('serie'),
        F.col('rooms'),
        F.col('condition'),
        F.col('ceiling_height')
    )

    return (
        df_bronze
        .withColumn("updated_at", F.current_timestamp())
        .dropDuplicates(["slug"])
        .filter(F.col("sqm_price") > 0)
        .filter(F.col("square") > 0)
        .filter(F.col("micro_district").isNotNull())
    )


def apply_anomaly_corrections(df: DataFrame) -> DataFrame:
    flagged = flag_anomalies(
        df,
        value_col="sqm_price",
        group_cols=["district", "rooms"],
        hard_low=100.0,
        hard_high=5000.0,
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
        logger.info(f"Apartment anomalies flagged: {len(anomaly_rows)}; invoking Bedrock (cap={max_llm_calls})")
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
            kind="apartment",
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

    return (
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
        .drop("corr_square", "corr_price_usd", "is_anomaly", "description", "price_usd")
    )


def apply_imputations_and_strict_filters(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("square", F.col("square").cast(T.IntegerType()))
        .withColumn(
            "kitchen_square",
            F.when(
                F.col("kitchen_square").isNull(),
                F.when(F.col("square") > 120, 15)
                .otherwise(
                    F.when(F.col("square") < 70, 6).otherwise(F.round(F.col("square") * 0.15).cast("int"))
                ),
            ).otherwise(F.col("kitchen_square.int")),
        )
        .withColumn(
            "ceiling_height",
            F.when(F.col("ceiling_height").isNull(), 3).otherwise(F.col("ceiling_height.double")),
        )
        .withColumn(
            "toilet",
            F.when(
                F.col("toilet").isNull(),
                F.when(F.col("square") > 140, 3).otherwise(F.when(F.col("square") > 75, 2).otherwise(1)),
            ).otherwise(F.col("toilet")),
        )
        .filter(F.col("sqm_price") > 300)
        .filter(F.col("sqm_price") < 2500)
    )


def update_scd2_table(*, key_field: T.StructField, parquet_path: str, compared_fields: List[T.StructField],
                      comparison_df: DataFrame, partition_col: str):
    # Add change columns for double type fields
    schema_fields = [key_field]  # Key

    for cf in compared_fields:
        schema_fields.append(cf)  # Original field
        # Add change column for double type fields
        if isinstance(cf.dataType, T.DoubleType):
            schema_fields.append(T.StructField(f"{cf.name}_change", T.DoubleType(), True))

    # Add SCD2 fields
    schema_fields.extend([
        T.StructField("effective_from", T.TimestampType(), False),  # SCD2 valid from
        T.StructField("effective_to", T.TimestampType(), True),  # SCD2 valid to
        T.StructField("is_current", T.BooleanType(), False)  # SCD2 current flag
    ])

    schema = T.StructType(schema_fields)
    try:
        current_scd2_df = spark.read.schema(schema).parquet(parquet_path)
    except:
        current_scd2_df = spark.createDataFrame([], schema)
    current_scd2_df.cache()
    # Join current prices with new data
    joined_df = comparison_df.alias("new").join(
        current_scd2_df.filter("is_current = true").alias("current"),
        comparison_df[key_field.name] == current_scd2_df[key_field.name],
        "full_outer"
    )

    # Handle potential null values in field comparisons
    fields_comparison_list = []
    for cf in compared_fields:
        fields_comparison_list.append(
            f"(new.{cf.name} IS NOT NULL AND current.{cf.name} IS NOT NULL AND new.{cf.name} != current.{cf.name}) OR " +
            f"(new.{cf.name} IS NULL AND current.{cf.name} IS NOT NULL) OR " +
            f"(new.{cf.name} IS NOT NULL AND current.{cf.name} IS NULL)"
        )
    fields_comparison_filter_str = ' OR '.join(fields_comparison_list)

    # Records to update (close existing current records)
    updates_select = [
        F.col(f"current.{key_field.name}")
    ]

    # Add fields and keep existing change values for double type fields
    for cf in compared_fields:
        updates_select.append(F.col(f"current.{cf.name}"))

        # Include existing change columns for double type fields
        if isinstance(cf.dataType, T.DoubleType):
            change_field_name = f"{cf.name}_change"
            updates_select.append(F.col(f"current.{change_field_name}"))

    # Add SCD2 fields
    updates_select.extend([
        F.col("current.effective_from"),
        F.current_timestamp().alias("effective_to"),
        F.lit(False).alias("is_current")
    ])

    updates_df = joined_df.where(
        f"current.{key_field.name} IS NOT NULL AND new.{key_field.name} IS NOT NULL AND ({fields_comparison_filter_str})"
    ).select(*updates_select)

    # New records (for changed values or new slugs)
    new_records_select = [
        F.coalesce(F.col(f"new.{key_field.name}"), F.col(f"current.{key_field.name}")).alias(key_field.name)
    ]

    # Add fields and calculate change for double type fields
    for cf in compared_fields:
        new_records_select.append(F.col(f"new.{cf.name}"))

        # Calculate percentage change for double type fields
        if isinstance(cf.dataType, T.DoubleType):
            change_field_name = f"{cf.name}_change"
            # Calculate percentage change: (new - old) / old * 100
            # If old value is null or 0, or new value is null, set change to 100.0 (100%)
            new_records_select.append(
                F.when(
                    (F.col(f"current.{cf.name}").isNull()) |
                    (F.col(f"current.{cf.name}") == 0) |
                    (F.col(f"new.{cf.name}").isNull()),
                    F.lit(0)  # 100% change
                ).otherwise(
                    ((F.col(f"new.{cf.name}") - F.col(f"current.{cf.name}")) /
                     F.col(f"current.{cf.name}") * 100.0)  # Calculate percentage change
                ).alias(change_field_name)
            )

    # Add SCD2 fields
    new_records_select.extend([
        F.current_timestamp().alias("effective_from"),
        F.lit(None).cast("timestamp").alias("effective_to"),
        F.lit(True).alias("is_current")
    ])

    new_records_df = joined_df.where(
        f"(current.{key_field.name} IS NULL) OR (current.{key_field.name} IS NOT NULL AND ({fields_comparison_filter_str}))"
    ).select(*new_records_select)

    # Keep unchanged current records
    unchanged_records_select = [
        F.col(f"current.{key_field.name}")
    ]

    # Add fields and keep existing change values for double type fields
    for cf in compared_fields:
        unchanged_records_select.append(F.col(f"current.{cf.name}").cast(cf.dataType))

        # Include existing change columns for double type fields
        if isinstance(cf.dataType, T.DoubleType):
            change_field_name = f"{cf.name}_change"
            unchanged_records_select.append(F.col(f"current.{change_field_name}"))

    # Add SCD2 fields
    unchanged_records_select.extend([
        F.col("current.effective_from"),
        F.col("current.effective_to"),
        F.col("current.is_current")
    ])

    unchanged_records_df = joined_df.where(
        f"current.{key_field.name} IS NOT NULL AND new.{key_field.name} IS NOT NULL AND NOT ({fields_comparison_filter_str})"
    ).select(*unchanged_records_select)

    # Historical records (not current)
    historical_records_df = current_scd2_df.filter("is_current = false")

    # Close entries when their {key_field.name} is not in the comparison dataframe
    to_close_select = [
        F.col(f"{key_field.name}")
    ]

    # Add fields and keep existing change values for double type fields
    for cf in compared_fields:
        to_close_select.append(F.col(cf.name))

        # Include existing change columns for double type fields
        if isinstance(cf.dataType, T.DoubleType):
            change_field_name = f"{cf.name}_change"
            to_close_select.append(F.col(change_field_name))

    # Add SCD2 fields with updated values
    to_close_select.extend([
        F.col("effective_from"),
        F.current_timestamp().alias("effective_to"),
        F.lit(False).alias("is_current")
    ])

    # Identify records in current_scd2_df that don't exist in comparison_df
    to_close_df = current_scd2_df.filter("is_current = true").alias("current").join(
        comparison_df.alias("new"),
        F.col(f"current.{key_field.name}") == F.col(f"new.{key_field.name}"),
        "left_anti"  # Keep only records from current_scd2_df that don't match in comparison_df
    ).select(*to_close_select)

    # Union all dataframes
    new_df = updates_df.unionAll(new_records_df).unionAll(unchanged_records_df).unionAll(
        historical_records_df).unionAll(to_close_df)
    new_df.write.mode("overwrite").parquet(parquet_path)
    new_df.unpersist()
    return spark.read.parquet(parquet_path)

def create_market_summary_table(cleaned_df_bronze):
    df_with_price = cleaned_df_bronze.withColumn("total_price", F.col("sqm_price") * F.col("square"))

    district_summary = df_with_price.groupBy("micro_district").agg(
        F.count("slug").cast(T.DoubleType()).alias("object_count"),
        F.sum("total_price").alias("total_price"),
        F.sum("square").alias("total_square")
    )

    market_summary = df_with_price.agg(
        F.lit(None).cast(T.StringType()).alias("micro_district"),
        F.count("slug").cast(T.DoubleType()).alias("object_count"),
        F.sum("total_price").alias("total_price"),
        F.sum("square").alias("total_square")
    )

    combined_summary = district_summary.unionAll(market_summary)

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
        "object_count",
        "total_price",
        "total_square",
        F.current_timestamp().alias("timestamp")
    )

    update_scd2_table(
        key_field=T.StructField("slug", T.StringType(), False),
        parquet_path=market_summary_s3_uri,
        compared_fields=fields,
        comparison_df=comparison_df,
        partition_col="is_current"
    )


def main():
    bronze_df = get_bronze_df().alias("incoming")
    bronze_df.cache()

    corrected_df = apply_anomaly_corrections(bronze_df)
    cleaned_df_bronze = apply_imputations_and_strict_filters(corrected_df)
    cleaned_df_bronze.cache()

    realty_dim_df = cleaned_df_bronze.drop("sqm_price")
    realty_dim_df.write.mode("overwrite").parquet(realty_dim_table_s3_uri)

    update_scd2_table(
        key_field=T.StructField("slug", T.StringType(), False),
        parquet_path=price_fact_s3_uri,
        compared_fields=[T.StructField("sqm_price", T.DoubleType(), False)],
        comparison_df=cleaned_df_bronze.select("slug", "sqm_price", F.col("updated_at").alias("timestamp")),
        partition_col="is_current"
    )

    create_market_summary_table(cleaned_df_bronze)


if __name__ == "__main__":
    main()
