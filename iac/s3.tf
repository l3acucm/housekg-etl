#######################
# S3 Bucket for data + MWAA
#######################
resource "aws_s3_bucket" "data_bucket" {
  bucket = "housekg-etl-bucket"
  force_destroy = true
}

resource "aws_s3_object" "feature_engineering_script" {
  bucket = aws_s3_bucket.data_bucket.bucket
  key    = "etl/feature_engineering.py"
  source = "../etl/jobs/glue/feature_engineering.py"
  etag = filebase64sha256("../etl/jobs/glue/feature_engineering.py")
}

resource "aws_s3_object" "model_training_script" {
  bucket = aws_s3_bucket.data_bucket.bucket
  key    = "etl/model_training.py"
  source = "../etl/jobs/sagemaker/model_training.py"
  etag = filebase64sha256("../etl/jobs/sagemaker/model_training.py")
}

resource "aws_s3_object" "model_deployment_script" {
  bucket = aws_s3_bucket.data_bucket.bucket
  key    = "etl/model_deployment.py"
  source = "../etl/jobs/sagemaker/model_deployment.py"
  etag = filebase64sha256("../etl/jobs/sagemaker/model_deployment.py")
}