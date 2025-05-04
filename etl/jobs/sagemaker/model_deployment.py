import os

import boto3
import time
from datetime import datetime

# Initialize SageMaker client
region = os.environ['AWS_REGION']
bucket_name = os.environ['BUCKET']
model_name_prefix = os.environ['MODEL_NAME_PREFIX']
timestamp_str = datetime.now().strftime("%Y%m%d")
model_name = f"{model_name_prefix}-{timestamp_str}"
sagemaker_client = boto3.client('sagemaker', region_name=region)

def deploy_model(model_s3_uri, role_arn):
    endpoint_config_name = f"housekg-endpoint-config-{timestamp_str}"
    endpoint_name = f"housekg-endpoint-{timestamp_str}"

    # Create SageMaker model
    sagemaker_client.create_model(
        ModelName=model_name,
        PrimaryContainer={
            "Image": "{var.aw}.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.3-1",
            "ModelDataUrl": model_s3_uri
        },
        ExecutionRoleArn=role_arn
    )

    # Create endpoint configuration
    sagemaker_client.create_endpoint_config(
        EndpointConfigName=endpoint_config_name,
        ProductionVariants=[
            {
                "VariantName": "default",
                "ModelName": model_name,
                "InstanceType": "ml.m5.large",
                "InitialInstanceCount": 1
            }
        ]
    )

    # Create endpoint
    sagemaker_client.create_endpoint(
        EndpointName=endpoint_name,
        EndpointConfigName=endpoint_config_name
    )

    # Wait for endpoint to be ready
    while sagemaker_client.describe_endpoint(EndpointName=endpoint_name)["EndpointStatus"] != "InService":
        time.sleep(30)

    return endpoint_name

if __name__ == "__main__":
    model_key = f"models/{model_name_prefix}-{timestamp_str}.pth"
    model_s3_uri = f"s3://{bucket_name}/{model_key}.tar.gz" # Update with your model path
    role_arn = "arn:aws:iam::ACCOUNT_ID:role/SageMakerExecutionRole" # Update with your SageMaker role ARN
    endpoint = deploy_model(model_s3_uri, role_arn)
    print(f"Model deployed at endpoint: {endpoint}")
