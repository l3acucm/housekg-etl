#######################
# AWS Lambda
#######################
resource "aws_lambda_function" "ingestion_lambda" {
  function_name = "ingestion_lambda"
  filename      = "artifacts/ingestion_lambda.zip"
  handler       = "main.handler"
  memory_size   = 512
  runtime       = "python3.10"
  role          = aws_iam_role.lambda_role.arn
  source_code_hash = filebase64sha256("artifacts/ingestion_lambda.zip")
  layers        = [aws_lambda_layer_version.requests_layer.arn]
  timeout       = 30
  environment {
    variables = {
      BUCKET_NAME = var.s3_bucket
      FILE_NAME_PREFIX = "apartments"
      CRAWLER_NAME = var.crawler_name
    }
  }
}

resource "aws_lambda_layer_version" "requests_layer" {
  filename   = "artifacts/requests_layer.zip"
  layer_name = "requests"
  source_code_hash = filebase64sha256("artifacts/requests_layer.zip")
  compatible_runtimes = ["python3.10"]
}

#######################
# CloudWatch Event Rule for Cron Trigger
#######################
resource "aws_cloudwatch_event_rule" "lambda_cron_rule" {
  name                = "ingestion-lambda-cron-rule"
  description         = "Triggers Ingestion Lambda every day at 1 AM UTC"
  schedule_expression = "cron(0 1 * * ? *)"  # Adjust cron expression as needed

  # Optional: If you want the rule to trigger immediately after being created
  # state = "ENABLED"
}

#######################
# CloudWatch Event Target to Trigger Lambda
#######################
resource "aws_cloudwatch_event_target" "lambda_cron_target" {
  rule      = aws_cloudwatch_event_rule.lambda_cron_rule.name
  arn       = aws_lambda_function.ingestion_lambda.arn
  target_id = "ingestion-lambda-target"
}

#######################
# Lambda Permissions to Allow CloudWatch Event to Invoke It
#######################
resource "aws_lambda_permission" "allow_cloudwatch_to_invoke_lambda" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.lambda_cron_rule.arn
}

