# Athena resources for querying crawled data

# Athena workgroup for querying the data
resource "aws_athena_workgroup" "housekg_workgroup" {
  name        = "housekg_workgroup"
  description = "Workgroup for querying HouseKG data"
  
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    
    result_configuration {
      output_location = "s3://${var.s3_bucket}/athena-results/"
      
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# Athena named query for realty price fact
resource "aws_athena_named_query" "price_fact_query" {
  name        = "price_fact_query"
  workgroup   = aws_athena_workgroup.housekg_workgroup.name
  database    = aws_glue_catalog_database.data_db.name
  description = "Query for realty price fact data"
  query       = "SELECT * FROM ${aws_glue_catalog_database.data_db.name}.realty_price_fact LIMIT 100;"
}

# Athena named query for realty dimension
resource "aws_athena_named_query" "realty_dim_query" {
  name        = "realty_dim_query"
  workgroup   = aws_athena_workgroup.housekg_workgroup.name
  database    = aws_glue_catalog_database.data_db.name
  description = "Query for realty dimension data"
  query       = "SELECT * FROM ${aws_glue_catalog_database.data_db.name}.realty_dim LIMIT 100;"
}

# Athena named query for market summary
resource "aws_athena_named_query" "market_summary_query" {
  name        = "market_summary_query"
  workgroup   = aws_athena_workgroup.housekg_workgroup.name
  database    = aws_glue_catalog_database.data_db.name
  description = "Query for market summary data"
  query       = "SELECT * FROM ${aws_glue_catalog_database.data_db.name}.market_summary LIMIT 100;"
}

# Athena named queries for plots
resource "aws_athena_named_query" "plots_dim_query" {
  name        = "plots_dim_query"
  workgroup   = aws_athena_workgroup.housekg_workgroup.name
  database    = aws_glue_catalog_database.data_db.name
  description = "Query for plots dimension data"
  query       = "SELECT * FROM ${aws_glue_catalog_database.data_db.name}.plots_dim LIMIT 100;"
}

resource "aws_athena_named_query" "plots_price_fact_query" {
  name        = "plots_price_fact_query"
  workgroup   = aws_athena_workgroup.housekg_workgroup.name
  database    = aws_glue_catalog_database.data_db.name
  description = "Query for plots price fact data"
  query       = "SELECT * FROM ${aws_glue_catalog_database.data_db.name}.plots_price_fact LIMIT 100;"
}

resource "aws_athena_named_query" "plots_market_summary_query" {
  name        = "plots_market_summary_query"
  workgroup   = aws_athena_workgroup.housekg_workgroup.name
  database    = aws_glue_catalog_database.data_db.name
  description = "Query for plots market summary data"
  query       = "SELECT * FROM ${aws_glue_catalog_database.data_db.name}.plots_market_summary LIMIT 100;"
}

# IAM role for Athena
resource "aws_iam_role" "athena_role" {
  name = "athena_role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "athena.amazonaws.com"
        }
      }
    ]
  })
}

# IAM policy for Athena
resource "aws_iam_role_policy" "athena_policy" {
  name = "athena_policy"
  role = aws_iam_role.athena_role.id
  
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload",
          "s3:PutObject"
        ],
        Resource = [
          "arn:aws:s3:::${var.s3_bucket}",
          "arn:aws:s3:::${var.s3_bucket}/*"
        ]
      }
    ]
  })
}