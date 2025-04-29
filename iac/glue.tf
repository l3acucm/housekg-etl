#######################
# Glue Catalog Database
#######################
resource "aws_glue_catalog_database" "data_db" {
  name = "realty_data"
}
resource "aws_glue_job" "bronze" {
  name        = "Bronze ETL job"
  role_arn    = aws_iam_role.glue_service_role.arn
  glue_version = "5.0" # Specify Glue version 5.0
  worker_type = "G.1X" # Example worker type
  number_of_workers = 2 # Number of workers
  timeout = 60 # Timeout in minutes
  max_retries = 1

  command {
    name = "glueetl" # For Spark ETL jobs
    script_location = "s3://${aws_s3_bucket.data_bucket.id}/${aws_s3_object.bronze_script.key}"
    python_version  = "3" # Python version supported in Glue 5.0
  }

  default_arguments = {
    "--job-language"          = "python"
    "--enable-metrics" = "true" # Enable metrics for job profiling
    "--enable-continuous-cloudwatch-log" = "true" # Enable continuous logging
    "--enable-spark-ui" = "true" # Enable Spark UI
    "--spark-event-logs-path" = "s3://${aws_s3_bucket.data_bucket.id}/spark-logs/"
    "--conf"                  = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
    "--conf"                  = "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    #"--requirements"                    = "s3://${aws_s3_bucket.data_bucket.id}/${aws_s3_object.requirements_txt.key}" # Path to requirements.txt
  }

  execution_property {
    max_concurrent_runs = 1 # Maximum concurrent runs
  }

  tags = {
    Name = "Glue Job"
  }
}