resource "aws_cloudwatch_event_rule" "s3_upload_trigger" {
  name        = "s3-file-upload-trigger"
  description = "Trigger on new object upload"
  event_pattern = jsonencode({
    "source": ["aws.s3"],
    "detail-type": ["Object Created"],
    "detail": {
      "bucket": {
        "name": [aws_s3_bucket.data_bucket.bucket]
      },
      eventName = ["PutObject"]
      "object": {
        "key": [{
          "prefix": "ingestions/prefix/"   # <== your filter here
        }]
      }
    }
  })
}

resource "aws_cloudwatch_event_target" "trigger_lambda" {
  rule = aws_cloudwatch_event_rule.s3_upload_trigger.name
  arn  = aws_lambda_function.start_glue_bronze.arn
}


resource "aws_cloudwatch_event_permission" "cloudwatch_event_permission" {
  principal    = "*"
  statement_id = "AllowCloudWatchToInvokeGlueJob"
  action       = "events:PutEvents"
}