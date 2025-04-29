from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import TensorDataset, DataLoader
from sklearn.model_selection import train_test_split

bucket = "housekg-etl-bucket"
bronze_key = f"bronze/"
silver_key = f"silver/"
timestamp_str = "29042025"
bronze_path = f"s3a://{bucket}/{bronze_key}/apartments_{timestamp_str}"
fact_table_name = "realty_price_fact_29042025"
fact_table_path = f"s3a://{bucket}/{silver_key}/apartments_{timestamp_str}"
realty_dim_table_path = f"s3a://{bucket}/{silver_key}/realty_dim_{timestamp_str}"
from awsglue.context import GlueContext
from pyspark.context import SparkContext

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
if not DeltaTable.isDeltaTable(spark, fact_table_path):
    schema = T.StructType([
        T.StructField("slug", T.StringType(), False),
        T.StructField("price", T.LongType(), True),
        T.StructField("effective_from", T.TimestampType(), False),
        T.StructField("effective_to", T.TimestampType(), True),
        T.StructField("is_current", T.BooleanType(), False)
    ])
    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta").mode("overwrite").save(fact_table_path)

# Read existing SCD2 table
delta_table = DeltaTable.forPath(spark, fact_table_path)
current_scd2_df = delta_table.toDF().where(F.col("is_current") == True).alias("current")

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
final_df = updates_df.union(new_records_df).union(unchanged_records_df)

# Perform merge operation
(delta_table
    .alias("target")
    .merge(
        final_df.alias("source"),
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

# Optimize table
delta_table.vacuum()
spark.sql(f"OPTIMIZE delta.`{fact_table_path}`")
cleaned_df_bronze = cleaned_df_bronze.drop('price')
cleaned_df_bronze.write.format("delta").mode("overwrite").save(realty_dim_table_path)

fact_df = DeltaTable.forPath(spark, fact_table_path).toDF()

training_df = cleaned_df_bronze.join(fact_df.where(F.col('is_current')==F.lit(True)), on='slug', how='inner').drop('effective_from', 'effective_to', 'is_current', 'year')

import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from pyspark.sql.functions import col, lit

# Assuming training_df is your PySpark DataFrame
# First, convert PySpark DataFrame to Pandas for easier PyTorch integration
pandas_df = training_df.toPandas()

# Define features
numerical_features = ['longitude', 'latitude', 'square', 'kitchen_square',
                      'rooms', 'ceiling_height']
categorical_features = ['district', 'micro_district', 'toilet', 'serie', 'condition']

# Define the preprocessing for numerical and categorical features
numerical_transformer = Pipeline(steps=[
    ('scaler', StandardScaler())
])

categorical_transformer = Pipeline(steps=[
    ('onehot', OneHotEncoder(handle_unknown='ignore'))
])

# Combine preprocessing steps
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numerical_transformer, numerical_features),
        ('cat', categorical_transformer, categorical_features)
    ])

# Preprocess the data
X = pandas_df.drop(['slug', 'price'], axis=1)
y = pandas_df['price']

# Split the data
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

# Fit and transform
X_train_preprocessed = preprocessor.fit_transform(X_train)
X_val_preprocessed = preprocessor.transform(X_val)

# Get shapes for model architecture
input_dim = X_train_preprocessed.shape[1]

# Convert to tensors
X_train_tensor = torch.FloatTensor(
    X_train_preprocessed.toarray() if hasattr(X_train_preprocessed, 'toarray') else X_train_preprocessed)
y_train_tensor = torch.FloatTensor(y_train.values).reshape(-1, 1)
X_val_tensor = torch.FloatTensor(
    X_val_preprocessed.toarray() if hasattr(X_val_preprocessed, 'toarray') else X_val_preprocessed)
y_val_tensor = torch.FloatTensor(y_val.values).reshape(-1, 1)


# Create a custom dataset
class HousingDataset(Dataset):
    def __init__(self, X, y):
        self.X = X
        self.y = y

    def __len__(self):
        return len(self.X)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]


# Create datasets and dataloaders
train_dataset = HousingDataset(X_train_tensor, y_train_tensor)
val_dataset = HousingDataset(X_val_tensor, y_val_tensor)

train_loader = DataLoader(train_dataset, batch_size=64, shuffle=True)
val_loader = DataLoader(val_dataset, batch_size=64, shuffle=False)


# Define the neural network model
class PricePredictor(nn.Module):
    def __init__(self, input_dim):
        super(PricePredictor, self).__init__()
        # Define layers with dropout for regularization
        self.model = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1)
        )

    def forward(self, x):
        return self.model(x)


# Initialize model, loss function, and optimizer
model = PricePredictor(input_dim)
criterion = nn.MSELoss()
optimizer = optim.Adam(model.parameters(), lr=0.001)


# Training function
def train_model(model, train_loader, val_loader, criterion, optimizer, epochs=1000):
    train_losses = []
    val_losses = []

    for epoch in range(epochs):
        # Training
        model.train()
        running_loss = 0.0
        for inputs, targets in train_loader:
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()
            running_loss += loss.item()

        epoch_train_loss = running_loss / len(train_loader)
        train_losses.append(epoch_train_loss)

        # Validation
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for inputs, targets in val_loader:
                outputs = model(inputs)
                loss = criterion(outputs, targets)
                val_loss += loss.item()

        epoch_val_loss = val_loss / len(val_loader)
        val_losses.append(epoch_val_loss)

        # Print progress every 10 epochs
        if (epoch + 1) % 10 == 0:
            print(f'Epoch {epoch + 1}/{epochs}, Train Loss: {epoch_train_loss:.4f}, Val Loss: {epoch_val_loss:.4f}')

    return train_losses, val_losses


# Train the model
print("Training the model...")
train_losses, val_losses = train_model(model, train_loader, val_loader, criterion, optimizer)
print("Training complete!")


# Function to make predictions on new data
def predict_prices(model, X_data, preprocessor):
    # Preprocess the data
    X_processed = preprocessor.transform(X_data)

    # Convert to tensor
    X_tensor = torch.FloatTensor(X_processed.toarray() if hasattr(X_processed, 'toarray') else X_processed)

    # Set model to evaluation mode
    model.eval()

    # Make predictions
    with torch.no_grad():
        predictions = model(X_tensor).numpy().flatten()

    return predictions


# Now, let's predict on the entire dataset and annotate the original DataFrame
print("Making predictions on the entire dataset...")
all_features = pandas_df.drop(['slug', 'price'], axis=1)
predicted_prices = predict_prices(model, all_features, preprocessor)

# Add predictions to the original pandas DataFrame
pandas_df['predicted_price'] = predicted_prices
print(pandas_df)