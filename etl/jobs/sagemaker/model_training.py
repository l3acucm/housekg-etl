# Imports
import os
import boto3
import pandas as pd
import io
from datetime import datetime

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split

# Get environment variables
model_name_prefix = os.environ.get('MODEL_NAME', 'price-predictor')
bucket_name = os.environ.get('BUCKET')
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


def load_data():
    print("Loading data from input channels...")

    # Use SageMaker environment variables to get input paths
    realty_dim_path = os.environ.get('SM_CHANNEL_REALTY_DIM')
    price_fact_path = os.environ.get('SM_CHANNEL_PRICE_FACT')

    # List all parquet files in the directories
    realty_dim_files = [os.path.join(realty_dim_path, f) for f in os.listdir(realty_dim_path)
                        if f.endswith('.parquet') or f.endswith('.parq')]
    price_fact_files = [os.path.join(price_fact_path, f) for f in os.listdir(price_fact_path)
                        if f.endswith('.parquet') or f.endswith('.parq')]

    # Read parquet files with pandas
    realty_dim_df = pd.concat([pd.read_parquet(f) for f in realty_dim_files])
    price_fact_df = pd.concat([pd.read_parquet(f) for f in price_fact_files])

    # Filter current prices
    price_fact_df = price_fact_df[price_fact_df['is_current'] == True]

    # Join the dataframes
    training_df = pd.merge(
        realty_dim_df,
        price_fact_df,
        on='slug',
        how='inner'
    )

    # Drop unnecessary columns
    training_df = training_df.drop(['longitude', 'latitude', 'is_current', 'effective_from', 'effective_to'], axis=1,
                                   errors='ignore')

    print(f"Loaded {len(training_df)} rows of data")
    return training_df


# Training function
def train_model(model, train_loader, val_loader, criterion, optimizer, epochs=20):
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
        # Get the default model directory for SageMaker
        model_dir = os.environ.get('SM_MODEL_DIR', '.')
        model_path = os.path.join(model_dir, 'model.pth')

        # Save model locally first (SageMaker requirement)
        torch.save(model.state_dict(), model_path)
        print(f"Model saved locally to {model_path}")

        # Also upload to our custom S3 location if bucket is provided
        if bucket_name:
            buffer = io.BytesIO()
            torch.save(model.state_dict(), buffer)
            buffer.seek(0)

            s3_client = boto3.client('s3')
            s3_client.upload_fileobj(buffer, bucket_name, model_key)
            print(f"Model also uploaded to s3://{bucket_name}/{model_key}")

        return model_path
    except Exception as e:
        print(f"Error saving model: {str(e)}")
        raise e


def main():
    # Get hyperparameters from the environment variables
    epochs = int(os.environ.get('SM_HP_EPOCHS', 100))
    learning_rate = float(os.environ.get('SM_HP_LEARNING_RATE', 0.001))
    batch_size = int(os.environ.get('SM_HP_BATCH_SIZE', 64))

    print(f"Starting training with {epochs} epochs, learning rate {learning_rate}, batch size {batch_size}")

    # Load the data
    training_df = load_data()

    # Feature setup
    numerical_features = ['square', 'kitchen_square', 'rooms', 'ceiling_height']
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
    X_train_tensor = torch.FloatTensor(
        X_train_preprocessed.toarray() if hasattr(X_train_preprocessed, 'toarray') else X_train_preprocessed)
    y_train_tensor = torch.FloatTensor(y_train.values).reshape(-1, 1)
    X_val_tensor = torch.FloatTensor(
        X_val_preprocessed.toarray() if hasattr(X_val_preprocessed, 'toarray') else X_val_preprocessed)
    y_val_tensor = torch.FloatTensor(y_val.values).reshape(-1, 1)

    # Create datasets and dataloaders
    train_dataset = HousingDataset(X_train_tensor, y_train_tensor)
    val_dataset = HousingDataset(X_val_tensor, y_val_tensor)

    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)

    # Initialize model, loss function, and optimizer
    model = PricePredictor(input_dim)
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)

    # Train the model
    print("Training the model...")
    train_losses, val_losses = train_model(model, train_loader, val_loader, criterion, optimizer, epochs=epochs)
    print("Training complete!")

    # Save the trained model
    save_model(model)

    print("Process completed successfully!")


if __name__ == "__main__":
    main()