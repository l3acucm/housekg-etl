resource "aws_sfn_state_machine" "data_processing_workflow" {
  name     = "data-processing-workflow"
  role_arn = aws_iam_role.step_function_role.arn

  definition = <<EOF
{
  "Comment": "Data Processing Workflow",
  "StartAt": "IngestData",
  "States": {
    "IngestData": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.ingestion_lambda.function_name}",
        "Payload.$": "$"
      },
      "Next": "RunGlueCrawler"
    },
    "RunGlueCrawler": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Parameters": {
        "Name": "${aws_glue_crawler.housekg_ingestions_crawler.name}"
      },
      "Next": "WaitForCrawler"
    },
    "WaitForCrawler": {
      "Type": "Wait",
      "Seconds": 60,
      "Next": "CheckCrawlerStatus"
    },
    "CheckCrawlerStatus": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler",
      "Parameters": {
        "Name": "${aws_glue_crawler.housekg_ingestions_crawler.name}"
      },
      "Next": "CrawlerStatusChoice"
    },
    "CrawlerStatusChoice": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.Crawler.State",
          "StringEquals": "RUNNING",
          "Next": "WaitForCrawler"
        }
      ],
      "Default": "StartGlueJob"
    },
    "StartGlueJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "${aws_glue_job.feature_engineering.name}"
      },
      "Next": "RunFinalCrawlers"
    },
    "RunFinalCrawlers": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "RunRealtyDimCrawler",
          "States": {
            "RunRealtyDimCrawler": {
              "Type": "Task",
              "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
              "Parameters": {
                "Name": "realty_dim"
              },
              "End": true
            }
          }
        },
        {
          "StartAt": "RunPriceFactCrawler",
          "States": {
            "RunPriceFactCrawler": {
              "Type": "Task",
              "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
              "Parameters": {
                "Name": "price_fact"
              },
              "End": true
            }
          }
        },
        {
          "StartAt": "RunPredictionFactCrawler",
          "States": {
            "RunPredictionFactCrawler": {
              "Type": "Task",
              "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
              "Parameters": {
                "Name": "prediction_fact"
              },
              "End": true
            }
          }
        },
        {
          "StartAt": "RunMarketSummaryCrawler",
          "States": {
            "RunMarketSummaryCrawler": {
              "Type": "Task",
              "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
              "Parameters": {
                "Name": "market_summary"
              },
              "End": true
            }
          }
        }
      ],
      "End": true
    }
  }
}
EOF
}

# Schedule the Step Function to run every hour
resource "aws_cloudwatch_event_rule" "daily_trigger" {
  name                = "data-processing-daily-trigger"
  description         = "Triggers data processing workflow every day"
  schedule_expression = "cron(0 1 * * ? *)"
}

resource "aws_cloudwatch_event_target" "step_function_target" {
  rule     = aws_cloudwatch_event_rule.daily_trigger.name
  arn      = aws_sfn_state_machine.data_processing_workflow.arn
  role_arn = aws_iam_role.cloudwatch_role.arn
}

resource "aws_iam_role" "cloudwatch_role" {
  name = "cloudwatch-step-function-trigger-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "cloudwatch_policy" {
  name = "cloudwatch-step-function-policy"
  role = aws_iam_role.cloudwatch_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = "states:StartExecution"
        Effect   = "Allow"
        Resource = aws_sfn_state_machine.data_processing_workflow.arn
      }
    ]
  })
}

resource "aws_iam_role" "step_function_role" {
  name = "step_function_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "step_function_policy" {
  name = "step_function_policy"
  role = aws_iam_role.step_function_role.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "lambda:InvokeFunction"
        ],
        Effect   = "Allow",
        Resource = aws_lambda_function.ingestion_lambda.arn
      },
      {
        Action = [
          "glue:StartCrawler",
          "glue:GetCrawler"
        ],
        Effect   = "Allow",
        Resource = "*"
      },
      {
        Action = [
          "glue:StartJobRun",
          "glue:UpdateCrawler",
          "glue:GetJobRun",
          "glue:BatchStopJobRun"
        ],
        Effect   = "Allow",
        Resource = "*"
      }
    ]
  })
}
