resource "aws_glue_catalog_database" "data_db" {
  name = "realty_data"
}

resource "aws_glue_crawler" "housekg_crawler" {
  name          = var.crawler_name
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.data_db.name
  classifiers = [aws_glue_classifier.housekg_json_classifier.name]

  s3_target {
    path = "s3://${aws_s3_bucket.data_bucket.bucket}/ingestions/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Tables = { AddOrUpdateBehavior = "MergeNewColumns" }
    }
  })
}
resource "aws_glue_classifier" "housekg_json_classifier" {
  name = "housekg_json_classifier"

  json_classifier {
    json_path = "$[*]"
  }
}
resource "aws_glue_job" "feature_engineering" {
  name              = "house_feature_engineering"
  role_arn          = aws_iam_role.glue_job_role.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  max_retries       = 0

  command {
    name = "glueetl" # Use "glueetl" for Spark ETL jobs
    script_location = "s3://${aws_s3_bucket.data_bucket.id}/${aws_s3_object.feature_engineering_script.key}"
    python_version  = "3" # Glue 5.0 supports Python 3.11
  }


  default_arguments = {
    "--BUCKET"                    = aws_s3_bucket.data_bucket.bucket
    "--BRONZE_KEY"                = "bronze/"
    "--SILVER_KEY"                = "silver/"
    "--job-language"              = "python"
    "--enable-metrics" = "true" # Enable metrics for job profiling
    "--enable-continuous-cloudwatch-log" = "true" # Enable continuous logging
    "--spark-event-logs-path"     = "s3://${aws_s3_bucket.data_bucket.id}/house-etl/feature-engineering/spark-logs/"
  }

  execution_property {
    max_concurrent_runs = 1 # Maximum concurrent runs
  }

}