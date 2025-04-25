#######################
# S3 Bucket for data + MWAA
#######################
resource "aws_s3_bucket" "data_bucket" {
  bucket = "housekg-etl-bucket"
  force_destroy = true
}

resource "aws_s3_object" "bronze_notebook" {
  bucket = aws_s3_bucket.data_bucket.bucket
  key    = "notebooks/bronze.ipynb"
  source = "../notebooks/bronze.ipynb"
  etag = filemd5("../notebooks/bronze.ipynb")
}

resource "aws_s3_object" "silver_notebook" {
  bucket = aws_s3_bucket.data_bucket.bucket
  key    = "notebooks/silver.ipynb"
  source = "../notebooks/silver.ipynb"
  etag = filemd5("../notebooks/silver.ipynb")
}