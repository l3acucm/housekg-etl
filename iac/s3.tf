resource "aws_s3_bucket" "data_bucket" {
  bucket        = var.s3_bucket
  force_destroy = true
}

resource "aws_s3_object" "feature_engineering_script" {
  bucket = aws_s3_bucket.data_bucket.bucket
  key    = "etl/feature_engineering.py"
  source = "../etl/jobs/glue/feature_engineering.py"
  etag   = filebase64sha256("../etl/jobs/glue/feature_engineering.py")
}

resource "aws_s3_object" "plots_feature_engineering_script" {
  bucket = aws_s3_bucket.data_bucket.bucket
  key    = "etl/plots_feature_engineering.py"
  source = "../etl/jobs/glue/plots_feature_engineering.py"
  etag   = filebase64sha256("../etl/jobs/glue/plots_feature_engineering.py")
}

resource "aws_s3_object" "anomaly_correction_module" {
  bucket = aws_s3_bucket.data_bucket.bucket
  key    = "etl/anomaly_correction.py"
  source = "../etl/jobs/glue/anomaly_correction.py"
  etag   = filebase64sha256("../etl/jobs/glue/anomaly_correction.py")
}