resource "aws_glue_catalog_database" "data_db" {
  name = "realty_data"
}

resource "aws_glue_crawler" "housekg_ingestions_crawler" {
  name          = "housekg_ingestions_crawler"
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
    "--BUCKET"                          = aws_s3_bucket.data_bucket.bucket
    "--job-language"                    = "python"
    "--enable-glue-datacatalog"         = "true"
    "--enable-metrics" = "true" # Enable metrics for job profiling
    "--enable-continuous-cloudwatch-log" = "true" # Enable continuous logging
    "--spark-event-logs-path"           = "s3://${aws_s3_bucket.data_bucket.id}/house-etl/feature-engineering/spark-logs/"
    "--additional-python-modules"       = "torch==2.0.1,scikit-learn==1.5.2"
    "--python-modules-installer-option" = "--extra-index-url https://download.pytorch.org/whl/cpu"
  }

  execution_property {
    max_concurrent_runs = 1 # Maximum concurrent runs
  }
}


resource "aws_glue_crawler" "price_fact" {
  name          = "price_fact"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.data_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_bucket.bucket}/silver/realty_price_fact"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Tables = { AddOrUpdateBehavior = "MergeNewColumns" }
    }
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}

resource "aws_glue_crawler" "realty_dim" {
  name          = "realty_dim_crawler"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.data_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_bucket.bucket}/silver/realty_dim"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Tables = { AddOrUpdateBehavior = "MergeNewColumns" }
    }
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}
resource "aws_glue_crawler" "prediction_fact" {
  name          = "prediction_fact"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.data_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_bucket.bucket}/silver/prediction_fact"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Tables = { AddOrUpdateBehavior = "MergeNewColumns" }
    }
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}