import sys
from typing import List

from pyspark.sql import functions as F, types as T, DataFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
import torch
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
import numpy as np
import boto3
import torch.nn as nn

args = getResolvedOptions(sys.argv, ['BUCKET'])
bucket = args['BUCKET']
silver_key = "silver"
glue_database_name = "realty_data"
glue_client = boto3.client('glue')
timestamp_str = datetime.now().strftime("%d%m%Y")

glue_context = GlueContext(SparkContext())
logger = glue_context.get_logger()
s3_client = boto3.client('s3')
spark = glue_context.spark_session
price_fact_table_name = f"realty_price_fact"
realty_dim_table_name = f"realty_dim"
prediction_fact_table_name = f"prediction_fact"

bronze_table_name = f"apartments_{timestamp_str}_json"
df_bronze = glue_context.create_dynamic_frame.from_catalog(
    database=glue_database_name,
    table_name=bronze_table_name
).toDF().select(F.explode(F.col("list")).alias("house")).select('house.*')

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
    F.col('condition'),
    F.col('ceiling_height')
)

cleaned_df_bronze = (df_bronze
                     .withColumn("square", F.col("square.int"))
                     .withColumn("kitchen_square",
                                 F.when(F.col("kitchen_square").isNull(),
                                        F.when(F.col("square") > 120, 15)
                                        .otherwise(F.when(F.col("square") < 70, 6)
                                                   .otherwise(F.round(F.col("square") * 0.15).cast("int"))))
                                 .otherwise(F.col("kitchen_square.int")))
                     .withColumn("ceiling_height",
                                 F.when(F.col("ceiling_height").isNull(), 3)
                                 .otherwise(F.col("ceiling_height.double")))
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

realty_dim_df = cleaned_df_bronze.drop("price")
realty_dim_df.write.mode("overwrite").parquet(f"s3://{bucket}/{silver_key}/{realty_dim_table_name}")


def get_scd2_table(parquet_path: str, fields: List[T.StructField], comparison_df: DataFrame):
    schema = T.StructType([
        T.StructField("slug", T.StringType(), False),  # Key
        *fields,  # Target value
        T.StructField("effective_from", T.TimestampType(), False),  # SCD2 valid from
        T.StructField("effective_to", T.TimestampType(), True),  # SCD2 valid to
        T.StructField("is_current", T.BooleanType(), False)  # SCD2 current flag
    ])
    try:
        current_scd2_df = spark.read.schema(schema).parquet(parquet_path)
    except:
        current_scd2_df = spark.createDataFrame([], schema)

    # Join current prices with new data
    joined_df = comparison_df.alias("new").join(
        current_scd2_df.filter("is_current = true").alias("current"),
        comparison_df["slug"] == current_scd2_df["slug"],
        "full_outer"
    )

    # Handle potential null values in field comparisons
    fields_comparison_list = []
    for field in fields:
        fields_comparison_list.append(
            f"(new.{field.name} IS NOT NULL AND current.{field.name} IS NOT NULL AND new.{field.name} != current.{field.name}) OR " +
            f"(new.{field.name} IS NULL AND current.{field.name} IS NOT NULL) OR " +
            f"(new.{field.name} IS NOT NULL AND current.{field.name} IS NULL)"
        )
    fields_comparison_filter_str = ' OR '.join(fields_comparison_list)

    # Records to update (close existing current records)
    updates_df = joined_df.where(
        f"current.slug IS NOT NULL AND new.slug IS NOT NULL AND ({fields_comparison_filter_str})"
    ).select(
        F.col("current.slug"),
        *[F.col(f"current.{field.name}") for field in fields],
        F.col("current.effective_from"),
        F.current_timestamp().alias("effective_to"),
        F.lit(False).alias("is_current")
    )

    # New records (for changed values or new slugs)
    new_records_df = joined_df.where(
        f"(current.slug IS NULL) OR (current.slug IS NOT NULL AND ({fields_comparison_filter_str}))"
    ).select(
        F.coalesce(F.col("new.slug"), F.col("current.slug")).alias("slug"),
        *[F.col(f"new.{field.name}") for field in fields],
        F.current_timestamp().alias("effective_from"),
        F.lit(None).cast("timestamp").alias("effective_to"),
        F.lit(True).alias("is_current")
    )

    # Keep unchanged current records
    unchanged_records_df = joined_df.where(
        f"current.slug IS NOT NULL AND new.slug IS NOT NULL AND NOT ({fields_comparison_filter_str})"
    ).select(
        F.col("current.slug"),
        *[F.col(f"current.{field.name}") for field in fields],
        F.col("current.effective_from"),
        F.col("current.effective_to"),
        F.col("current.is_current")
    )

    # Historical records (not current)
    historical_records_df = current_scd2_df.filter("is_current = false")

    # Union all dataframes
    return updates_df.unionAll(new_records_df).unionAll(unchanged_records_df).unionAll(
        historical_records_df)


price_fact_s3_uri = f"s3://{bucket}/{silver_key}/{price_fact_table_name}/"
price_fact_df = get_scd2_table(
    price_fact_s3_uri,
    [T.StructField("price", T.DoubleType(), False)],
    cleaned_df_bronze.select("slug", "price", F.col("updated_at").alias("timestamp"))
)
price_fact_df.write.mode("overwrite").parquet(price_fact_s3_uri)

price_fact_pd_df = price_fact_df.toPandas()
realty_dim_pd_df = realty_dim_df.toPandas()

current_prices_pd_df = price_fact_pd_df[price_fact_pd_df['is_current'] == True].drop(
    columns=["effective_from", "effective_to", "is_current"])
realty_dim_pd_df = realty_dim_pd_df.drop(columns=["updated_at", "longitude", "latitude"])

merged_pd_df = realty_dim_pd_df.merge(current_prices_pd_df, on="slug").dropna(subset=['square'])
cat_cols = ["serie", "district", "micro_district", "condition", "toilet"]
num_cols = ["square", "kitchen_square", "year", "ceiling_height"]
target_col = "price"

# Encode categoricals
encoders = {}
for col in cat_cols:
    merged_pd_df[col] = merged_pd_df[col].astype(str).fillna("missing")
    enc = LabelEncoder()
    merged_pd_df[col] = enc.fit_transform(merged_pd_df[col])
    encoders[col] = enc

# Fill missing numerics
for col in num_cols:
    merged_pd_df[col] = merged_pd_df[col].fillna(merged_pd_df[col].median())

# Scale numerics
scaler = StandardScaler()
merged_pd_df[num_cols] = scaler.fit_transform(merged_pd_df[num_cols])

# Prepare tensors
X = merged_pd_df[cat_cols + num_cols].astype(np.float32).values
y = merged_pd_df["price"].astype(np.float32).values.reshape(-1, 1)

X_tensor = torch.tensor(X)
y_tensor = torch.tensor(y)

X_train, X_val, y_train, y_val = train_test_split(X_tensor, y_tensor, test_size=0.2, random_state=42)

# Model
model = nn.Sequential(
    nn.Linear(X.shape[1], 64),
    nn.ReLU(),
    nn.Linear(64, 32),
    nn.ReLU(),
    nn.Linear(32, 1)
)

optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
loss_fn = nn.MSELoss()

# Train
for epoch in range(15000):
    model.train()
    optimizer.zero_grad()
    y_pred = model(X_train)
    loss = loss_fn(y_pred, y_train)
    loss.backward()
    optimizer.step()

    if epoch % 1000 == 0 or epoch == 14999:
        with torch.no_grad():
            val_loss = loss_fn(model(X_val), y_val).item()
        print(f"Epoch {epoch}, Train Loss: {loss.item():.4f}, Val Loss: {val_loss:.4f}")

# Predict and add to df
model.eval()
with torch.no_grad():
    predictions = model(torch.tensor(X, dtype=torch.float32)).numpy().flatten()

merged_pd_df["price"] = predictions
# Denormalize numeric features (optional)
merged_pd_df[num_cols] = scaler.inverse_transform(merged_pd_df[num_cols])

predictions_pd_df = merged_pd_df[["slug", "price"]]
predictions_df = spark.createDataFrame(predictions_pd_df).withColumn("timestamp", F.current_timestamp())
prediction_fact_s3_uri = f"s3://{bucket}/{silver_key}/{prediction_fact_table_name}/"
prediction_fact_df = get_scd2_table(
    prediction_fact_s3_uri,
    [T.StructField("price", T.DoubleType(), False)],
    predictions_df
)
prediction_fact_df.write.mode("overwrite").parquet(prediction_fact_s3_uri)