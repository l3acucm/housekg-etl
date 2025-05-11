variable "aws_region" {
  description = "AWS region"
  type        = string
}
variable "s3_bucket" {
  description = "S3 Bucket"
  type        = string
}
variable "bronze_table_prefix" {
  description = "Bronze table prefix"
  type        = string
}
variable "ingestions_dir" {
  description = "Ingestions directory name"
  type        = string
}
variable "ingestions_table_name" {
  description = "Ingestions directory name"
  type        = string
}
variable "aws_account_id" {
  description = "AWS Account ID"
  type        = string
}
variable "database_name" {
  description = "DB name"
  type        = string
}
variable "model_name" {
  description = "Model name"
  type        = string
}