#######################
# S3 Bucket for data + MWAA
#######################
resource "aws_s3_bucket" "data_bucket" {
  bucket = "housekg-etl-bucket"
  force_destroy = true
}
