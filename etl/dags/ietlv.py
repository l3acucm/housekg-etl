from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerTrainingOperator,
    SageMakerModelOperator,
    SageMakerEndpointConfigOperator,
    SageMakerEndpointOperator
)
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import boto3
import json
aws_region = 'us-east-1'
aws_account_id = 'AIDA6GBMCO3FDKE4T4225'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    'ml_pipeline_glue_sagemaker',
    default_args=default_args,
    description='ML pipeline with Glue ETL and SageMaker training',
    schedule_interval='@daily',
    catchup=False
)

# Configuration for Glue ingestion job
ingestion_job_name = 'ml_data_ingestion'
ingestion_job_params = {
    '--source_data_path': 's3://source-bucket/raw-data/',
    '--destination_path': 's3://processed-bucket/ingested-data/',
    '--job_bookmark': 'job-bookmark-enable'
}

# Configuration for Glue feature engineering job
feature_eng_job_name = 'ml_feature_engineering'
feature_eng_job_params = {
    '--data_path': 's3://processed-bucket/ingested-data/',
    '--feature_path': 's3://processed-bucket/feature-store/',
    '--enable_cleansing': 'true'
}

# SageMaker training configuration
training_job_config = {
    "AlgorithmSpecification": {
        "TrainingImage": f"{aws_account_id}.dkr.ecr.{{region}}.amazonaws.com/custom-ml-algorithm:latest",
        "TrainingInputMode": "File"
    },
    "RoleArn": f"arn:aws:iam::{aws_account_id}:role/SageMakerExecutionRole",
    "OutputDataConfig": {
        "S3OutputPath": "s3://processed-bucket/models/"
    },
    "ResourceConfig": {
        "InstanceCount": 1,
        "InstanceType": "ml.m5.xlarge",
        "VolumeSizeInGB": 30
    },
    "StoppingCondition": {
        "MaxRuntimeInSeconds": 3600
    },
    "InputDataConfig": [
        {
            "ChannelName": "train",
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": "s3://processed-bucket/feature-store/train/",
                    "S3DataDistributionType": "FullyReplicated"
                }
            }
        },
        {
            "ChannelName": "validation",
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": "s3://processed-bucket/feature-store/validation/",
                    "S3DataDistributionType": "FullyReplicated"
                }
            }
        }
    ],
    "HyperParameters": {
        "epochs": "10",
        "learning-rate": "0.1"
    }
}

model_config = {
    "ExecutionRoleArn": f"arn:aws:iam::{aws_account_id}:role/SageMakerExecutionRole",
    "ModelName": "ml-model-{{ ds_nodash }}",
    "PrimaryContainer": {
        "Image": f"{aws_account_id}.dkr.ecr.{aws_region}.amazonaws.com/custom-ml-algorithm:latest",
        "ModelDataUrl": "{{ ti.xcom_pull(task_ids='sagemaker_training')['Training']['ModelArtifacts']['S3ModelArtifacts'] }}"
    }
}

# Endpoint configuration
endpoint_config_config = {
    "EndpointConfigName": "ml-endpoint-config-{{ ds_nodash }}",
    "ProductionVariants": [
        {
            "VariantName": "variant-1",
            "ModelName": "ml-model-{{ ds_nodash }}",
            "InitialInstanceCount": 1,
            "InstanceType": "ml.t2.medium"
        }
    ]
}

# Endpoint configuration
endpoint_config = {
    "EndpointName": "ml-endpoint-{{ ds_nodash }}",
    "EndpointConfigName": "ml-endpoint-config-{{ ds_nodash }}"
}

# Define tasks

# 1. Glue ingestion job
ingest_data = GlueJobOperator(
    task_id='ingest_data',
    job_name=ingestion_job_name,
    script_args=ingestion_job_params,
    dag=dag
)

# 2. Glue feature engineering job
feature_engineering = GlueJobOperator(
    task_id='feature_engineering',
    job_name=feature_eng_job_name,
    script_args=feature_eng_job_params,
    dag=dag
)

# 3. SageMaker training job
train_model = SageMakerTrainingOperator(
    task_id='sagemaker_training',
    config=training_job_config,
    wait_for_completion=True,
    print_log=True,
    dag=dag
)

# 4. Create SageMaker model
create_model = SageMakerModelOperator(
    task_id='create_model',
    config=model_config,
    dag=dag
)

# 5. Create endpoint configuration
create_endpoint_config = SageMakerEndpointConfigOperator(
    task_id='create_endpoint_config',
    config=endpoint_config_config,
    dag=dag
)

# 6. Create or update endpoint
deploy_endpoint = SageMakerEndpointOperator(
    task_id='deploy_endpoint',
    config=endpoint_config,
    wait_for_completion=True,
    dag=dag
)

# Define task dependencies
ingest_data >> feature_engineering >> train_model >> create_model # >> create_endpoint_config >> deploy_endpoint