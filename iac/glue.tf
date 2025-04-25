#######################
# Glue Catalog Database
#######################
resource "aws_glue_catalog_database" "data_db" {
  name = "realty_data"
}

resource "aws_glue_job" "glue_bronze_job" {
  name     = "bronze_etl_job"
  role_arn     = aws_iam_role.glue_service_role.arn
  command {
    name            = "bronze"
    script_location = "s3://${aws_s3_bucket.data_bucket.bucket}/notebooks/bronze.ipynb"  # Glue script that calls notebooks
  }
  max_capacity = 10

  # Optionally, use a Glue Spark context for your job
  default_arguments = {
    "--TempDir" = "s3://${aws_s3_bucket.data_bucket.bucket}/glue_temp_dir/"
  }
}

resource "aws_glue_job" "glue_silver_job" {
  name     = "silver_etl_job"
  role_arn     = aws_iam_role.glue_service_role.arn
  command {
    name            = "silver"
    script_location = "s3://${aws_s3_bucket.data_bucket.bucket}/notebooks/silver.ipynb"  # Glue script that calls notebooks
  }
  max_capacity = 10

  # Optionally, use a Glue Spark context for your job
  default_arguments = {
    "--TempDir" = "s3://${aws_s3_bucket.data_bucket.bucket}/glue_temp_dir/"
  }
}
