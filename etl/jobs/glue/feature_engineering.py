import sys

from pyspark.sql import functions as F, types as T
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from datetime import datetime
import boto3


args = getResolvedOptions(sys.argv, ['BUCKET'])
bucket = args['BUCKET']
silver_key = "silver"
glue_database_name = "realty_data"

timestamp_str = datetime.now().strftime("%d%m%Y")

glue_context = GlueContext(SparkContext())
logger = glue_context.get_logger()
s3_client = boto3.client('s3')
spark = glue_context.spark_session
price_fact_table_name = f"realty_price_fact_{timestamp_str}"
realty_dim_table_name = f"realty_dim_{timestamp_str}"
price_fact_table_key = f"{silver_key}/{price_fact_table_name}"
realty_dim_table_key = f"{silver_key}/{realty_dim_table_name}"

bronze_table_name = "apartments_07052025_json"
df_bronze = glue_context.create_dynamic_frame.from_catalog(
    database=glue_database_name,
    table_name=bronze_table_name
).toDF().select(F.explode(F.col("list")).alias("house")).select('house.*')
scd2_schema = T.StructType([
    T.StructField("slug", T.StringType(), False),  # Key
    T.StructField("price", T.DoubleType(), False),  # Target value
    T.StructField("effective_from", T.TimestampType(), False),  # SCD2 valid from
    T.StructField("effective_to", T.TimestampType(), True),    # SCD2 valid to
    T.StructField("is_current", T.BooleanType(), False)      # SCD2 current flag
])


def read_or_create_df():
    try:
        # Check if file exists in S3
        s3_client.head_object(Bucket=bucket, Key=price_fact_table_key)

        # Read Parquet from S3
        df = spark.read.parquet(f"s3a://{bucket}/{price_fact_table_key}")

    except s3_client.exceptions.ClientError:
        # Create new Spark DataFrame with SCD2 schema if file doesn't exist
        df = spark.createDataFrame([], schema=scd2_schema)

        # Save to S3 as Parquet
        df.write.mode("overwrite").parquet(f"s3a://{bucket}/{price_fact_table_key}")

    return df


# Usage
df_bronze = df_bronze.select(
    F.col('slug'),
    F.col('longitude'),
    F.col('latitude'),
    F.col('prices')[1]['price'].alias('price'),
    F.col('square'),
    F.col('kitchen_square'),
    F.col('district'),
    F.col('micro_district'),
    F.col('updated_at'),
    F.col('year'),
    F.col('toilet'),
    F.col('serie'),
    F.col('rooms'),
    # F.col('parking'),
    # F.col('material'),
    # F.col('heating'),
    # F.col('gas'),
    # F.col('furniture'),
    # F.col('floor'),
    # F.col('flooring'),
    F.col('condition'),
    # F.col('balcony'),
    F.col('ceiling_height')
)

cleaned_df_bronze = (df_bronze
                     .withColumn("kitchen_square",
                                 F.when(F.col("kitchen_square").isNull(),
                                        F.when(F.col("square.int") > 120, 15)
                                        .otherwise(F.when(F.col("square.int") < 70, 6)
                                                   .otherwise(F.round(F.col("square.int") * 0.15).cast("int"))))
                                 .otherwise(F.col("kitchen_square.int")))
                     .withColumn("ceiling_height",
                                 F.when(F.col("ceiling_height").isNull(), 3)
                                 .otherwise(F.col("ceiling_height.double")))
                     .withColumn("toilet",
                                 F.when(F.col("toilet").isNull(),
                                        F.when(F.col("square.int") > 140, 3)
                                        .otherwise(F.when(F.col("square.int") > 75, 2)
                                                   .otherwise(1)))
                                 .otherwise(F.col("toilet")))
                     .withColumn("updated_at", F.current_timestamp())
                     .dropDuplicates(["slug"])
                     .alias("incoming")
                     )
realty_dim_df = cleaned_df_bronze.drop("price")
current_price_fact_df = read_or_create_df()

# Join current prices with new data
joined_df = cleaned_df_bronze.alias("new").join(
    current_price_fact_df.filter("is_current = true").alias("current"),
    cleaned_df_bronze["slug"] == current_price_fact_df["slug"],
    "full_outer"
)

# Update records where price changed
updates_df = joined_df.where(
    "current.slug IS NOT NULL AND new.price != current.price"
).select(
    F.col("current.slug"),
    F.col("current.price"),
    F.col("current.effective_from"),
    F.current_timestamp().alias("effective_to"),
    F.lit(False).alias("is_current")
)

# New records for changed prices or new slugs
new_records_df = joined_df.where(
    "current.slug IS NULL OR new.price != current.price"
).select(
    F.col("new.slug"),
    F.col("new.price"),
    F.current_timestamp().alias("effective_from"),
    F.lit(None).cast("timestamp").alias("effective_to"),
    F.lit(True).alias("is_current")
)

# Keep unchanged current records
unchanged_records_df = joined_df.where(
    "current.slug IS NOT NULL AND new.price = current.price"
).select(
    F.col("current.slug"),
    F.col("current.price"),
    F.col("current.effective_from"),
    F.col("current.effective_to"),
    F.col("current.is_current")
)

# Historical records (not current)
historical_records_df = current_price_fact_df.filter("is_current = false")

updated_price_fact_df = updates_df.unionAll(new_records_df).unionAll(unchanged_records_df).unionAll(historical_records_df)


updated_price_fact_df.write.mode("overwrite").parquet(f's3://{bucket}/{price_fact_table_key}')
realty_dim_df.write.mode("overwrite").parquet(f's3://{bucket}/{realty_dim_table_key}')
