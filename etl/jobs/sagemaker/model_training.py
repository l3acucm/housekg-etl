# Imports
import os
from datetime import datetime

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from awsglue.context import GlueContext
from pyspark.sql import functions as F
import io
import boto3

model_name_prefix = os.environ['MODEL_NAME']
role_arn = os.environ['ROLE_ARN']
region = os.environ['AWS_REGION']
bucket_name = os.environ['BUCKET']
xgboost_version = os.environ.get('XGBOOST_VERSION', '1.3-1')

timestamp_str = datetime.now().strftime("%d.%m.%Y")
model_key = f"models/{model_name_prefix}-{timestamp_str}.pth"


# Create a custom dataset
class HousingDataset(Dataset):
    def __init__(self, X, y):
        self.X = X
        self.y = y

    def __len__(self):
        return len(self.X)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]


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

bronze_key = f"bronze/"
silver_key = f"silver/"
timestamp_str = "29042025"
price_fact_table_path = f"s3a://{bucket_name}/{silver_key}/realty_price_fact_{timestamp_str}"
realty_dim_table_path = f"s3a://{bucket_name}/{silver_key}/realty_dim_{timestamp_str}"
price_fact_df = spark.read.format("parquet").load(price_fact_table_path)
realty_dim_df = spark.read.format("parquet").load(realty_dim_table_path)
prediction_fact_table_path = f"s3a://{bucket_name}/{silver_key}/prediction_fact_{timestamp_str}"

# Prepare training df
training_df = realty_dim_df.join(
    price_fact_df.where(F.col('is_current')==F.lit(True)),
    on='slug',
    how='inner'
).drop('longitude', 'latitude', 'is_current', 'effective_from', 'effective_to').toPandas()

# Feature setup
numerical_features = ['square', 'kitchen_square',
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
X = training_df.drop(['slug', 'price'], axis=1)
y = training_df['price']

# Split the data
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

# Fit and transform
X_train_preprocessed = preprocessor.fit_transform(X_train)
X_val_preprocessed = preprocessor.transform(X_val)

# Get shapes for model architecture
input_dim = X_train_preprocessed.shape[1]

# Convert to tensors
X_train_tensor = torch.FloatTensor(X_train_preprocessed.toarray() if hasattr(X_train_preprocessed, 'toarray') else X_train_preprocessed)
y_train_tensor = torch.FloatTensor(y_train.values).reshape(-1, 1)
X_val_tensor = torch.FloatTensor(X_val_preprocessed.toarray() if hasattr(X_val_preprocessed, 'toarray') else X_val_preprocessed)
y_val_tensor = torch.FloatTensor(y_val.values).reshape(-1, 1)

# Create datasets and dataloaders
train_dataset = HousingDataset(X_train_tensor, y_train_tensor)
val_dataset = HousingDataset(X_val_tensor, y_val_tensor)

train_loader = DataLoader(train_dataset, batch_size=64, shuffle=True)
val_loader = DataLoader(val_dataset, batch_size=64, shuffle=False)

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


def save_model(model):
    try:
        # Save model to a BytesIO object instead of a file
        buffer = io.BytesIO()
        torch.save(model.state_dict(), buffer)

        # Reset buffer position to beginning
        buffer.seek(0)

        # Upload directly to S3
        s3_client = boto3.client('s3')
        s3_client.upload_fileobj(buffer, bucket_name, model_key)

        print(f"Model successfully uploaded to s3://{bucket_name}/{model_key}")
        return f"s3://{bucket_name}/{model_key}"

    except Exception as e:
        print(f"Error saving model to S3: {str(e)}")
        raise e

# Train the model
print("Training the model...")
train_losses, val_losses = train_model(model, train_loader, val_loader, criterion, optimizer)
print("Training complete!")
save_model(model)
