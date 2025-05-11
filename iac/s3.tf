#######################
# S3 Bucket for data + MWAA
#######################
resource "aws_s3_bucket" "data_bucket" {
  bucket = var.s3_bucket
  force_destroy = true
}

resource "aws_s3_object" "feature_engineering_script" {
  bucket = aws_s3_bucket.data_bucket.bucket
  key    = "etl/feature_engineering.py"
  source = "../etl/jobs/glue/feature_engineering.py"
  etag = filebase64sha256("../etl/jobs/glue/feature_engineering.py")
}
