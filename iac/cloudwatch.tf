resource "aws_cloudwatch_event_rule" "s3_upload_trigger" {
  name        = "s3-file-upload-trigger"
  description = "Trigger Glue job when a new file is uploaded to the S3 bucket"
  event_pattern = jsonencode({
    source = ["aws.s3"]
    detail-type = ["AWS API Call via CloudTrail"]
    detail = {
      eventSource = ["s3.amazonaws.com"]
      eventName = ["PutObject"]
      requestParameters = {
        bucketName = [aws_s3_bucket.data_bucket.bucket]
      }
    }
  })
}

resource "aws_cloudwatch_event_target" "trigger_bronze_job" {
  rule = aws_cloudwatch_event_rule.s3_upload_trigger.name
  arn  = "arn:aws:glue:us-east-1:975050012362:job/bronze_etl_job"

  input = jsonencode({
    "JobName" = aws_glue_job.glue_bronze_job.name
  })
}


resource "aws_cloudwatch_event_permission" "cloudwatch_event_permission" {
  principal    = "*"
  statement_id = "AllowCloudWatchToInvokeGlueJob"
  action       = "events:PutEvents"
}