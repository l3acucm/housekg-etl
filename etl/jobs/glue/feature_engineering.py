import sys

from pyspark.sql import functions as F, types as T
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
scd2_schema = T.StructType([
    T.StructField("slug", T.StringType(), False),  # Key
    T.StructField("price", T.DoubleType(), False),  # Target value
    T.StructField("effective_from", T.TimestampType(), False),  # SCD2 valid from
    T.StructField("effective_to", T.TimestampType(), True),  # SCD2 valid to
    T.StructField("is_current", T.BooleanType(), False)  # SCD2 current flag
])

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
try:
    current_price_fact_df = spark.read.schema(scd2_schema).parquet(
        f"s3://{bucket}/{silver_key}/{price_fact_table_name}/")
except:
    current_price_fact_df = spark.createDataFrame([], scd2_schema)

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
updated_price_fact_df = updates_df.unionAll(new_records_df).unionAll(unchanged_records_df).unionAll(
    historical_records_df)

price_fact_pd_df = updated_price_fact_df.toPandas()
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
for epoch in range(3000):
    model.train()
    optimizer.zero_grad()
    y_pred = model(X_train)
    loss = loss_fn(y_pred, y_train)
    loss.backward()
    optimizer.step()

    if epoch % 100 == 0 or epoch == 2999:
        with torch.no_grad():
            val_loss = loss_fn(model(X_val), y_val).item()
        print(f"Epoch {epoch}, Train Loss: {loss.item():.4f}, Val Loss: {val_loss:.4f}")

# Predict and add to df
model.eval()
with torch.no_grad():
    predictions = model(torch.tensor(X, dtype=torch.float32)).numpy().flatten()

merged_pd_df["predicted_price"] = predictions

# Denormalize numeric features (optional)
merged_pd_df[num_cols] = scaler.inverse_transform(merged_pd_df[num_cols])
predictions_pd_df = merged_pd_df[["slug", "predicted_price"]]

predictions_df = spark.createDataFrame(predictions_pd_df).withColumn("timestamp", F.current_timestamp())

updated_price_fact_df.write.mode("overwrite").parquet(f"s3://{bucket}/{silver_key}/{price_fact_table_name}")
realty_dim_df.write.mode("overwrite").parquet(f"s3://{bucket}/{silver_key}/{realty_dim_table_name}")
predictions_df.write.mode("overwrite").parquet(f"s3://{bucket}/{silver_key}/{prediction_fact_table_name}")
