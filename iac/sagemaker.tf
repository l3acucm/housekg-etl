# resource "aws_sagemaker_training_job" "realty_model_training" {
#   name = "realty-price-prediction-training"
#
#   role_arn = aws_iam_role.sagemaker_role.arn
#
#   algorithm_specification {
#     training_image = "382416733822.dkr.ecr.us-west-2.amazonaws.com/pytorch-training:1.9.1-cpu-py38-ubuntu18.04" # Example PyTorch image
#     training_input_mode = "File"
#   }
#
#   input_data_config {
#     channel_name = "training"
#     content_type = "application/x-parquet"
#     s3_data_source {
#       s3_data_type = "S3Prefix"
#       s3_uri = "s3://your-bucket/silver/realty_price_fact_${timestamp_str}/"
#     }
#   }
#
#   output_data_config {
#     s3_uri = "s3://your-bucket/silver/model-output/"
#   }
#
#   resource_config {
#     instance_type = "ml.m5.xlarge"
#     instance_count = 1
#     volume_size_in_gb = 50
#   }
#
#   stopping_condition {
#     max_runtime_in_seconds = 3600
#     max_wait_time_in_seconds = 3600
#   }
#
#   hyper_parameters = {
#     "batch_size" = "32"
#     "epochs" = "10"
#   }
#
#   tags = {
#     "Project" = "RealtyPrediction"
#     "Environment" = "Production"
#   }
# }
#
# # 3. Deploy Model after training (optional)
# resource "aws_sagemaker_model" "realty_model" {
#   name = "realty-price-prediction-model"
#   execution_role_arn = aws_iam_role.sagemaker_role.arn
#
#   primary_container {
#     image = "382416733822.dkr.ecr.us-west-2.amazonaws.com/pytorch-inference:1.9.1-cpu-py38-ubuntu18.04"
#     model_data_url = "s3://your-bucket/silver/model-output/model.pth"
#   }
# }
#
# resource "aws_sagemaker_endpoint" "realty_endpoint" {
#   endpoint_name = "realty-price-prediction-endpoint"
#   endpoint_config_name = aws_sagemaker_endpoint_configuration.realty_endpoint_config.name
# }
#
# resource "aws_sagemaker_endpoint_configuration" "realty_endpoint_config" {
#   name = "realty-price-prediction-endpoint-config"
#
#   production_variants {
#     variant_name = "AllTraffic"
#     model_name   = aws_sagemaker_model.realty_model.name
#     initial_instance_count = 1
#     instance_type = "ml.m5.xlarge"
#   }
# }
