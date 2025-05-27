import sys
from typing import List
from pyspark.sql import functions as F, types as T, DataFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
import numpy as np
from torch import nn
import boto3
import torch
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

glue_context = GlueContext(SparkContext())
args = getResolvedOptions(sys.argv, ['BUCKET'])
bucket = args['BUCKET']
logger = glue_context.get_logger()
spark = glue_context.spark_session
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

silver_key = "silver"
glue_database_name = "realty_data"
glue_client = boto3.client('glue')
timestamp_str = datetime.now().strftime("%d%m%Y")

s3_client = boto3.client('s3')
price_fact_table_name = f"realty_price_fact"
realty_dim_table_name = f"realty_dim"
prediction_fact_table_name = f"prediction_price_fact"
market_summary_table_name = f"market_summary"
realty_dim_table_s3_uri = f"s3://{bucket}/{silver_key}/{realty_dim_table_name}"
price_fact_s3_uri = f"s3://{bucket}/{silver_key}/{price_fact_table_name}"
prediction_fact_s3_uri = f"s3://{bucket}/{silver_key}/{prediction_fact_table_name}"
market_summary_s3_uri = f"s3://{bucket}/{silver_key}/{market_summary_table_name}"

bronze_table_name = f"apartments_{timestamp_str}_json"

cat_cols = ["serie", "district", "micro_district", "condition", "toilet"]
num_cols = ["year", "ceiling_height"]
target_col = "sqm_price"


# Define a simple neural network model
class HousePriceModel(nn.Module):
    def __init__(self, input_dim):
        super(HousePriceModel, self).__init__()
        self.layer1 = nn.Linear(input_dim, 64)
        self.layer2 = nn.Linear(64, 32)
        self.layer3 = nn.Linear(32, 1)
        self.relu = nn.ReLU()

    def forward(self, x):
        x = self.relu(self.layer1(x))
        x = self.relu(self.layer2(x))
        x = self.layer3(x)
        return x


def get_cleaned_bronze_df():
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

    return (df_bronze
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
            .withColumn("sqm_price", F.col("price") / F.col("square"))
            .filter(F.col("sqm_price") > 300)
            .filter(F.col("sqm_price") < 2500)
            .filter(F.col("micro_district").isNotNull())
            .drop("price")
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


def replace_fact_price_with_predicted_in_df(df):
    categorical_transformer = Pipeline(steps=[
        ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
    ])
    numerical_transformer = Pipeline(steps=[
        ('scaler', StandardScaler())
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numerical_transformer, num_cols),
            ('cat', categorical_transformer, cat_cols)
        ])

    # Fit and transform the data
    X_all = preprocessor.fit_transform(df)
    input_dim = X_all.shape[1]
    predictions = np.zeros(len(df))

    # Recommend optimal hyperparameters based on dataset size
    dataset_size = len(df)

    # For small datasets (around 1500 samples), we need to be careful about overfitting
    # Recommended learning rate and epochs based on dataset size
    if dataset_size <= 1000:
        learning_rate = 0.01  # Higher learning rate for very small datasets
        epochs = 500  # Fewer epochs to prevent overfitting
    elif dataset_size <= 2000:  # This covers our case of 1500
        learning_rate = 0.005  # Moderate learning rate for small datasets
        epochs = 750  # Moderate number of epochs
    else:
        learning_rate = 0.001  # Lower learning rate for larger datasets
        epochs = 1000  # More epochs for better convergence

    print(f"Dataset size: {dataset_size}")
    print(f"Recommended learning rate: {learning_rate}")
    print(f"Recommended epochs: {epochs}")

    # Loop through each row
    for i in range(len(df)):
        # Create train set excluding current row
        train_idx = [j for j in range(len(df)) if j != i]
        test_idx = [i]

        # Train set without current row
        X_train = X_all[train_idx]
        y_train = df.iloc[train_idx][target_col].values

        # Test set (current row only)
        X_test = X_all[test_idx]

        # Create model for this iteration
        model = HousePriceModel(input_dim)

        # Convert to tensors
        X_train_tensor = torch.FloatTensor(X_train)
        y_train_tensor = torch.FloatTensor(y_train).view(-1, 1)
        X_test_tensor = torch.FloatTensor(X_test)

        # Handle NaNs
        if torch.isnan(X_train_tensor).any():
            X_train_tensor = torch.nan_to_num(X_train_tensor)
        if torch.isnan(X_test_tensor).any():
            X_test_tensor = torch.nan_to_num(X_test_tensor)

        # Train model
        criterion = nn.MSELoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)

        # Training loop
        for epoch in range(epochs):
            # Forward pass
            y_pred = model(X_train_tensor)
            loss = criterion(y_pred, y_train_tensor)

            # Backward pass and optimize
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        # Make prediction for current row
        model.eval()
        with torch.no_grad():
            predictions[i] = model(X_test_tensor).numpy().flatten()[0]

        # Print progress
        if i % 100 == 0 or i == len(df) - 1:
            print(f"Processed {i}/{len(df)} samples")

    df[target_col] = predictions
    return df


def create_market_summary_table(cleaned_df_bronze):
    # Add total_price column (sqm_price * square)
    df_with_price = cleaned_df_bronze.withColumn("total_price", F.col("sqm_price") * F.col("square"))

    # Group by micro_district
    district_summary = df_with_price.groupBy("micro_district").agg(
        F.count("slug").cast(T.DoubleType()).alias("object_count"),  # Cast to double for change tracking
        F.sum("total_price").alias("total_price"),
        F.sum("square").alias("total_square")
    )

    # Create a row for the whole market (null micro_district)
    market_summary = df_with_price.agg(
        F.lit(None).cast(T.StringType()).alias("micro_district"),
        F.count("slug").cast(T.DoubleType()).alias("object_count"),  # Cast to double for change tracking
        F.sum("total_price").alias("total_price"),
        F.sum("square").alias("total_square")
    )

    # Combine district and market summaries
    combined_summary = district_summary.unionAll(market_summary)

    # Create a unique slug for each row (micro_district + date or "market" + date)
    combined_summary = combined_summary.withColumn(
        "slug",
        F.when(
            F.col("micro_district").isNull(),
            F.lit("market")
        ).otherwise(
            F.col("micro_district")
        )
    ).drop('micro_district')

    # Use update_scd2_table to track changes
    fields = [
        T.StructField("object_count", T.DoubleType(), False),  # Using DoubleType to enable change tracking
        T.StructField("total_price", T.DoubleType(), False),
        T.StructField("total_square", T.DoubleType(), False)
    ]

    # Create comparison DataFrame for update_scd2_table
    comparison_df = combined_summary.select(
        "slug",
        "object_count",
        "total_price",
        "total_square",
        F.current_timestamp().alias("timestamp")
    )

    # Update the SCD2 table to track changes
    market_summary_df = update_scd2_table(
        key_field=T.StructField("slug", T.StringType(), False),
        parquet_path=market_summary_s3_uri,
        compared_fields=fields,
        comparison_df=comparison_df,
        partition_col="is_current"
    )

    return market_summary_df


def main():
    cleaned_df_bronze = get_cleaned_bronze_df()
    realty_dim_df = cleaned_df_bronze.drop("sqm_price")
    realty_dim_df.write.mode("overwrite").parquet(realty_dim_table_s3_uri)
    price_fact_df = update_scd2_table(
        key_field=T.StructField("slug", T.StringType(), False),
        parquet_path=price_fact_s3_uri,
        compared_fields=[T.StructField("sqm_price", T.DoubleType(), False)],
        comparison_df=cleaned_df_bronze.select("slug", "sqm_price", F.col("updated_at").alias("timestamp")),
        partition_col="is_current"
    )

    # Create and save market summary table
    market_summary_df = create_market_summary_table(cleaned_df_bronze)

    price_fact_pd_df = price_fact_df.toPandas()
    realty_dim_pd_df = realty_dim_df.toPandas()

    current_prices_pd_df = price_fact_pd_df[price_fact_pd_df['is_current'] == True].drop(
        columns=["effective_from", "effective_to", "is_current"])
    realty_dim_pd_df = realty_dim_pd_df.drop(columns=["updated_at", "longitude", "latitude"])

    training_df = realty_dim_pd_df.merge(current_prices_pd_df, on="slug")
    predicted_prices_pd_df = replace_fact_price_with_predicted_in_df(training_df)
    update_scd2_table(
        key_field=T.StructField("slug", T.StringType(), False),
        parquet_path=prediction_fact_s3_uri,
        compared_fields=[T.StructField("sqm_price", T.DoubleType(), False)],
        comparison_df=spark.createDataFrame(predicted_prices_pd_df),
        partition_col="is_current"
    )


if __name__ == "__main__":
    main()
