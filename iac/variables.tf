variable "aws_region" {
  description = "AWS region"
  type        = string
}
variable "s3_bucket" {
  description = "S3 Bucket"
  type        = string
}
variable "crawler_name" {
  description = "Crawler name"
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
variable "redshift_admin_username" {
  description = "Redshift admin"
  type        = string
}
variable "redshift_admin_password" {
  description = "Redshift admin password"
  type        = string
}
variable "redshift_database_name" {
  description = "Redshift DB name"
  type        = string
}