
resource "aws_iam_policy" "custom_mwaa_policy" {
  name        = "CustomMWAAServiceRolePolicy"
  description = "Custom policy for MWAA service role"
  policy      = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "arn:aws:s3:::your-bucket-name/*"
      },
      {
        Effect   = "Allow"
        Action   = "logs:*"
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = "glue:*"
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = "airflow:*"
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "mwaa_exec_role" {
  name = "mwaa_exec_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "airflow-env.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "mwaa_basic_execution" {
  role       = aws_iam_role.mwaa_exec_role.name
  policy_arn = aws_iam_policy.custom_mwaa_policy.arn
}

resource "aws_iam_policy" "mwaa_s3_access" {
  name        = "MWAAS3AccessPolicy"
  description = "Policy to allow MWAA access to the S3 bucket for DAGs"
  policy      = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::housekg-etl-bucket/dags/*",  # Allow access to the DAGs folder
          "arn:aws:s3:::housekg-etl-bucket"          # Allow listing the bucket
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "mwaa_s3_access_attachment" {
  role       = aws_iam_role.mwaa_exec_role.name
  policy_arn = aws_iam_policy.mwaa_s3_access.arn
}

resource "aws_iam_policy" "mwaa_s3_public_access_block" {
  name        = "MWAAS3PublicAccessBlockPolicy"
  description = "Policy to allow MWAA to check public access block configuration"
  policy      = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:GetAccountPublicAccessBlock"
        Action   = "s3:GetBucketPublicAccessBlock"
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "mwaa_s3_public_access_block_attachment" {
  role       = aws_iam_role.mwaa_exec_role.name
  policy_arn = aws_iam_policy.mwaa_s3_public_access_block.arn
}