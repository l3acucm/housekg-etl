#######################
# S3 Bucket for data + MWAA
#######################
resource "aws_s3_bucket" "data_bucket" {
  bucket = "housekg-etl-bucket"
  force_destroy = true
}

resource "aws_s3_object" "bronze_script" {
  bucket = aws_s3_bucket.data_bucket.bucket
  key    = "etl/bronze.py"
  source = "../etl/bronze.py"
  etag = filemd5("../etl/bronze.py")
}