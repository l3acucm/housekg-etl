variable "aws_region" {
  description = "AWS region"
  type        = string
}
variable "s3_bucket" {
  description = "S3 Bucket"
  type        = string
}
variable "webhook_url" {
  description = "Endpoint POSTed with newline-separated URLs of newly-discovered listings that match the notification criteria"
  type        = string
}