#######################
# MWAA Environment
#######################
resource "aws_mwaa_environment" "airflow_env" {
  name                 = "realty-airflow"
  airflow_version      = "2.8.1"
  execution_role_arn = aws_iam_role.mwaa_exec_role.arn # Replace with proper MWAA role
  source_bucket_arn    = aws_s3_bucket.data_bucket.arn
  dag_s3_path          = "dags"
  #requirements_s3_path = "requirements.txt"
  environment_class = "mw1.small"
  min_workers       = 1
  max_workers       = 1

  logging_configuration {
    dag_processing_logs {
      enabled   = false
      log_level = "CRITICAL"
    }
    scheduler_logs {
      enabled   = false
      log_level = "CRITICAL"
    }
    task_logs {
      enabled   = true
      log_level = "ERROR"
    }
    webserver_logs {
      enabled   = false
      log_level = "CRITICAL"
    }
    worker_logs {
      enabled   = false
      log_level = "CRITICAL"
    }
  }


  network_configuration {
    security_group_ids = [aws_security_group.mwaa_sg.id]
    subnet_ids = [aws_subnet.subnet_a.id, aws_subnet.subnet_b.id]
  }
}