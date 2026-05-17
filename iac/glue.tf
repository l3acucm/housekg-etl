resource "aws_iam_role" "glue_crawler_role" {
  name = "glue_crawler_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }

    ]
  })
}

resource "aws_iam_role_policy" "glue_crawler_policy" {
  role = aws_iam_role.glue_crawler_role.name
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetDatabase",
          "glue:UpdateDatabase",
          "glue:CreatePartition",
          "glue:GetTable"
        ]
        Resource = "*" # Use "*" for actions that don't support resource-level permissions
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket}",
          "arn:aws:s3:::${var.s3_bucket}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role" "glue_job_role" {
  name = "glue_job_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
    ]
  })
}


resource "aws_iam_role_policy" "glue_job_policy" {
  role = aws_iam_role.glue_job_role.name
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        "Effect" : "Allow",
        "Action" : [
          "sagemaker:CreateTransformJob",
          "sagemaker:DescribeTransformJob",
          "sagemaker:DescribeModel"
        ],
        "Resource" : [
          "*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetConnection",
          "glue:UseConnection"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:*"
        ]
        Resource = [
          aws_s3_bucket.data_bucket.arn,
          "${aws_s3_bucket.data_bucket.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:*"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = "arn:aws:s3:::your-bucket-name/*"
      },
      {
        Effect = "Allow"
        Action = [
          "redshift-serverless:GetWorkgroup",
          "redshift-serverless:GetNamespace"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "bedrock:InvokeModel"
        ]
        Resource = [
          "arn:aws:bedrock:*::foundation-model/anthropic.claude-haiku-4-*",
          "arn:aws:bedrock:*:*:inference-profile/us.anthropic.claude-haiku-4-*",
          "arn:aws:bedrock:*:*:inference-profile/eu.anthropic.claude-haiku-4-*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ],
        "Resource" : [
          "arn:aws:s3:::housekg-etl",
          "arn:aws:s3:::housekg-etl/*"
        ]
      }
    ]
  })
}
resource "aws_glue_catalog_database" "data_db" {
  name = "realty_data"
}

resource "aws_glue_crawler" "housekg_ingestions_crawler" {
  name          = "housekg_ingestions_crawler"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.data_db.name
  classifiers   = [aws_glue_classifier.housekg_json_classifier.name]

  s3_target {
    path = "s3://${aws_s3_bucket.data_bucket.bucket}/ingestions_apartments/"
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
  execution_class   = "FLEX"
  number_of_workers = 2
  timeout           = 60
  max_retries       = 0

  command {
    name            = "glueetl" # Use "glueetl" for Spark ETL jobs
    script_location = "s3://${aws_s3_bucket.data_bucket.id}/${aws_s3_object.feature_engineering_script.key}"
    python_version  = "3" # Glue 5.0 supports Python 3.11
  }


  default_arguments = {
    "--BUCKET"                           = aws_s3_bucket.data_bucket.bucket
    "--job-language"                     = "python"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true" # Enable metrics for job profiling
    "--enable-continuous-cloudwatch-log" = "true" # Enable continuous logging
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.data_bucket.id}/house-etl/feature-engineering/spark-logs/"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.data_bucket.id}/${aws_s3_object.anomaly_correction_module.key}"
    "--MODEL_ID"                         = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
    "--BEDROCK_REGION"                   = "us-east-1"
    "--MAX_LLM_CALLS"                    = "100"
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
  name          = "realty_dim"
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
resource "aws_glue_crawler" "market_summary" {
  name          = "market_summary"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.data_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_bucket.bucket}/silver/market_summary"
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

resource "aws_glue_crawler" "plots_ingestions_crawler" {
  name          = "plots_ingestions_crawler"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.data_db.name
  classifiers   = [aws_glue_classifier.housekg_json_classifier.name]

  s3_target {
    path = "s3://${aws_s3_bucket.data_bucket.bucket}/ingestions_plots/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Tables = { AddOrUpdateBehavior = "MergeNewColumns" }
    }
  })
}

resource "aws_glue_job" "plots_feature_engineering" {
  name              = "plots_feature_engineering"
  role_arn          = aws_iam_role.glue_job_role.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  execution_class   = "FLEX"
  number_of_workers = 2
  timeout           = 60
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_bucket.id}/${aws_s3_object.plots_feature_engineering_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--BUCKET"                           = aws_s3_bucket.data_bucket.bucket
    "--job-language"                     = "python"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.data_bucket.id}/house-etl/plots-feature-engineering/spark-logs/"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.data_bucket.id}/${aws_s3_object.anomaly_correction_module.key}"
    "--MODEL_ID"                         = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
    "--BEDROCK_REGION"                   = "us-east-1"
    "--MAX_LLM_CALLS"                    = "100"
  }

  execution_property {
    max_concurrent_runs = 1
  }
}

resource "aws_glue_crawler" "plots_dim" {
  name          = "plots_dim"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.data_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_bucket.bucket}/silver/plots_dim"
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

resource "aws_glue_crawler" "plots_price_fact" {
  name          = "plots_price_fact"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.data_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_bucket.bucket}/silver/plots_price_fact"
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

resource "aws_glue_crawler" "plots_market_summary" {
  name          = "plots_market_summary"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.data_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_bucket.bucket}/silver/plots_market_summary"
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
