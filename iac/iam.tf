#######################
# IAM
#######################
resource "aws_iam_role" "lambda_exec_role" {
  name = "lambda_exec_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}


resource "aws_iam_policy" "lambda_s3_policy" {
  name = "lambda-s3-access"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = ["s3:PutObject"],
        Effect   = "Allow",
        Resource = "${aws_s3_bucket.data_bucket.arn}/*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "cloudwatch_event_policy" {
  name = "cloudwatch_event_policy"
  role = aws_iam_role.glue_service_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "glue:StartJobRun"
        Resource = aws_glue_job.glue_bronze_job.arn
      },
      {
        Effect   = "Allow"
        Action   = "glue:StartJobRun"
        Resource = aws_glue_job.glue_silver_job.arn
      }
    ]
  })
}

# IAM Role for Glue Service
resource "aws_iam_role" "glue_service_role" {
  name = "glue_service_role"
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

resource "aws_iam_policy" "cloudwatch_event_policy" {
  name   = "cloudwatch_event_policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "glue:StartJobRun",
      Effect = "Allow",
      Resource = aws_glue_job.glue_bronze_job.arn
    },
    {
      Action = "glue:StartJobRun",
      Effect = "Allow",
      Resource = aws_glue_job.glue_silver_job.arn
    }]
  })
}

resource "aws_iam_role" "cloudwatch_event_role" {
  name = "cloudwatch_event_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action    = "sts:AssumeRole",
      Effect    = "Allow",
      Principal = {
        Service = "events.amazonaws.com"
      }
    }]
  })
}


resource "aws_iam_role_policy_attachment" "lambda_s3_attach" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.lambda_s3_policy.arn
}


resource "aws_iam_role_policy_attachment" "glue_s3_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_logs_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_cloudwatch_logs" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_cloudwatch_events" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.cloudwatch_event_policy.arn
}

# Attach policies to the Glue service role
resource "aws_iam_role_policy_attachment" "glue_role_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "cloudwatch_event_policy_attachment" {
  role       = aws_iam_role.cloudwatch_event_role.name
  policy_arn = aws_iam_policy.cloudwatch_event_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}