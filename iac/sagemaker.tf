resource "aws_sagemaker_pipeline" "housekg-price-regression" {
  pipeline_name         = "realty-price-prediction-pipeline"
  pipeline_display_name = "realty-price-prediction-pipeline"
  role_arn              = aws_iam_role.sagemaker_pipeline_role.arn

  pipeline_definition = jsonencode({
    Version = "2020-12-01"
    Parameters = [
      {
        Name         = "ModelName"
        Type         = "String"
        DefaultValue = "realty-price-predictor"
      },
      {
        Name         = "TrainingInstanceType"
        Type         = "String"
        DefaultValue = "ml.c5.xlarge"
      },
      {
        Name         = "DeploymentInstanceType"
        Type         = "String"
        DefaultValue = "ml.t2.medium"
      },
      {
        Name         = "Epochs"
        Type         = "String"
        DefaultValue = "100"
      },
      {
        Name         = "BatchSize"
        Type         = "String"
        DefaultValue = "64"
      },
      {
        Name         = "LearningRate"
        Type         = "String"
        DefaultValue = "0.001"
      },
      {
        Name         = "TrainingImage"
        Type         = "String"
        DefaultValue = "763104351884.dkr.ecr.us-east-1.amazonaws.com/pytorch-training:1.13.1-gpu-py39"
      },
      {
        Name         = "InferenceImage"
        Type         = "String"
        DefaultValue = "763104351884.dkr.ecr.us-east-1.amazonaws.com/pytorch-inference:1.13.1-gpu-py39"
      },
      {
        Name         = "DimTableS3Uri"
        Type         = "String"
        DefaultValue = "s3://${aws_s3_bucket.data_bucket.bucket}/silver/realty_dim_07052025/"
      },
      {
        Name         = "PriceFactTableS3Uri"
        Type         = "String"
        DefaultValue = "s3://${aws_s3_bucket.data_bucket.bucket}/silver/realty_price_fact_07052025/"
      }
    ]
    Steps = [
      {
        Name = "TrainingJob"
        Type = "Training"
        Arguments = {
          AlgorithmSpecification = {
            TrainingImage = { "Get" = "Parameters.TrainingImage" }
            TrainingInputMode                = "File"
            EnableSageMakerMetricsTimeSeries = true
            MetricDefinitions = [
              {
                Name  = "train:loss"
                Regex = "Train Loss: ([0-9\\.]+)"
              },
              {
                Name  = "val:loss"
                Regex = "Val Loss: ([0-9\\.]+)"
              }
            ]
          }
          HyperParameters = {
            epochs = { "Get" = "Parameters.Epochs" }
            learning-rate = { "Get" = "Parameters.LearningRate" }
            batch-size = { "Get" = "Parameters.BatchSize" }
            sagemaker_program = "model_training.py"
            sagemaker_submit_directory = "s3://${aws_s3_bucket.data_bucket.bucket}/etl/model_training.tar.gz"
            MODEL_NAME = { "Get" = "Parameters.ModelName" }
            BUCKET = aws_s3_bucket.data_bucket.bucket
          }
          InputDataConfig = [
            {
              ChannelName = "realty_dim"
              DataSource = {
                S3DataSource = {
                  S3Uri = { "Get" = "Parameters.DimTableS3Uri" }
                  S3DataType             = "S3Prefix"
                  S3DataDistributionType = "FullyReplicated"
                }
              }
              ContentType = "application/x-parquet"
            },
            {
              ChannelName = "price_fact"
              DataSource = {
                S3DataSource = {
                  S3Uri = { "Get" = "Parameters.PriceFactTableS3Uri" }
                  S3DataType             = "S3Prefix"
                  S3DataDistributionType = "FullyReplicated"
                }
              }
              ContentType = "application/x-parquet"
            }
          ]
          OutputDataConfig = {
            S3OutputPath = "s3://${aws_s3_bucket.data_bucket.bucket}/model-output/"
          }
          ResourceConfig = {
            InstanceType = { "Get" = "Parameters.TrainingInstanceType" }
            InstanceCount  = 1
            VolumeSizeInGB = 30
          }
          StoppingCondition = {
            MaxRuntimeInSeconds  = 3600
            MaxWaitTimeInSeconds = 3600
          }
          EnableManagedSpotTraining = true
          RoleArn = aws_iam_role.sagemaker_pipeline_role.arn
          Environment = {
            SAGEMAKER_PROGRAM = "model_training.py"
          }
          DebugHookConfig = {
            S3OutputPath = "s3://${aws_s3_bucket.data_bucket.bucket}/debug-output"
          }
        }
      },
      {
        Name = "CreateModel"
        Type = "Model"
        Arguments = {
          ExecutionRoleArn = aws_iam_role.sagemaker_pipeline_role.arn
          PrimaryContainer = {
            ModelDataUrl = { "Get" = "Steps.TrainingJob.ModelArtifacts.S3ModelArtifacts" }
            Image = { "Get" = "Parameters.InferenceImage" }
          }
          ModelName = { "Get" = "Parameters.ModelName" }
        }
      },
      {
        Name = "DeployEndpoint"
        Type = "EndpointConfig"
        Arguments = {
          EndpointConfigName = { "Get" = "Parameters.ModelName" }
          ProductionVariants = [
            {
              ModelName = { "Get" = "Parameters.ModelName" }
              VariantName          = "AllTraffic"
              InitialInstanceCount = 1
              InstanceType = { "Get" = "Parameters.DeploymentInstanceType" }
            }
          ]
        }
      },
      {
        Name = "Deploy"
        Type = "Endpoint"
        Arguments = {
          EndpointName = { "Get" = "Parameters.ModelName" }
          EndpointConfigName = { "Get" = "Steps.DeployEndpoint.EndpointConfigName" }
        }
      }
    ]
  })
}