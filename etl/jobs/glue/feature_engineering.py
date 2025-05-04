import sys

from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from datetime import datetime
args = getResolvedOptions(sys.argv, ['BUCKET'])
bucket = args['BUCKET']
bronze_key = 'bronze'
silver_key = 'silver'

timestamp_str = datetime.now().strftime("%d.%m.%Y")

glueContext = GlueContext(SparkContext())
spark = glueContext.spark_session
bronze_path = f"s3a://{bucket}/{bronze_key}/apartments_{timestamp_str}"
price_fact_table_path = f"s3a://{bucket}/{silver_key}/realty_price_fact_{timestamp_str}"
realty_dim_table_path = f"s3a://{bucket}/{silver_key}/realty_dim_{timestamp_str}"

glueContext = GlueContext(SparkContext())
spark = glueContext.spark_session

df_bronze = spark.read.format("delta").load(bronze_path)

cleaned_df_bronze = (df_bronze
    .withColumn("kitchen_square",
        F.when(F.col("kitchen_square").isNull(),
            F.when(F.col("square") > 120, 15)
            .otherwise(F.when(F.col("square") < 70, 6)
            .otherwise(F.round(F.col("square") * 0.15).cast("int"))))
        .otherwise(F.col("kitchen_square")))
    .withColumn("ceiling_height",
        F.when(F.col("ceiling_height").isNull(), 3)
        .otherwise(F.col("ceiling_height")))
    .withColumn("toilet",
        F.when(F.col("toilet").isNull(),
            F.when(F.col("square") > 140, 3)
            .otherwise(F.when(F.col("square") > 75, 2)
            .otherwise(1)))
        .otherwise(F.col("toilet")))
    .withColumn("updated_at", F.current_timestamp())
    .dropDuplicates(["slug"])
    .alias("incoming")
)

#Initialize Delta table if it doesn't exist
if not DeltaTable.isDeltaTable(spark, price_fact_table_path):
    schema = T.StructType([
        T.StructField("slug", T.StringType(), False),
        T.StructField("price", T.LongType(), True),
        T.StructField("effective_from", T.TimestampType(), False),
        T.StructField("effective_to", T.TimestampType(), True),
        T.StructField("is_current", T.BooleanType(), False)
    ])
    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta").mode("overwrite").save(price_fact_table_path)

# Read existing SCD2 table
price_fact_delta_table = DeltaTable.forPath(spark, price_fact_table_path)
current_scd2_df = price_fact_delta_table.toDF().where(F.col("is_current") == True).alias("current")

# Join incoming with current records
join_cond = [cleaned_df_bronze["slug"] == current_scd2_df["slug"]]
joined_df = cleaned_df_bronze.join(current_scd2_df, join_cond, "full_outer")

# Prepare updates for existing records (close old records)
updates_df = (joined_df
    .where(current_scd2_df["slug"].isNotNull() &
           (cleaned_df_bronze["price"] != current_scd2_df["price"]))
    .select(
        current_scd2_df["slug"],
        current_scd2_df["price"],
        current_scd2_df["effective_from"],
        F.current_timestamp().alias("effective_to"),
        F.lit(False).alias("is_current")
    ))

# Prepare new records
new_records_df = (joined_df
    .where(current_scd2_df["slug"].isNull() |
           (cleaned_df_bronze["price"] != current_scd2_df["price"]))
    .select(
        cleaned_df_bronze["slug"],
        cleaned_df_bronze["price"],
        F.current_timestamp().alias("effective_from"),
        F.lit(None).cast("timestamp").alias("effective_to"),
        F.lit(True).alias("is_current")
    ))

# Prepare unchanged records
unchanged_records_df = (joined_df
    .where((cleaned_df_bronze["price"] == current_scd2_df["price"]) &
           current_scd2_df["is_current"] == True)
    .select(
        current_scd2_df["slug"],
        current_scd2_df["price"],
        current_scd2_df["effective_from"],
        current_scd2_df["effective_to"],
        current_scd2_df["is_current"]
    ))

# Combine all records
final_price_fact_df = updates_df.union(new_records_df).union(unchanged_records_df)

# Writes for delta/glue/databricks
(price_fact_delta_table
    .alias("target")
    .merge(
        final_price_fact_df.alias("source"),
        "target.slug = source.slug AND target.is_current = true"
    )
    .whenMatchedUpdate(set={
        "effective_to": "source.effective_to",
        "is_current": "source.is_current"
    })
    .whenNotMatchedInsert(values={
        "slug": "source.slug",
        "price": "source.price",
        "effective_from": "source.effective_from",
        "effective_to": "source.effective_to",
        "is_current": "source.is_current"
    })
    .execute())



cleaned_df_bronze.drop("price").write.mode("overwrite").parquet(realty_dim_table_path)
final_price_fact_df.write.mode("overwrite").parquet(price_fact_table_path)
# writing for glue
# cleaned_df_bronze.drop('price').write.format("delta").mode("overwrite").save(realty_dim_table_path)
# final_price_fact_df.write.format("delta").mode("overwrite").save(price_fact_table_path)
