terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.96"
    }
  }

  backend "s3" {
    bucket = "realty-etl-tf-backend"
    key    = "mvp.tfstate"
    region = "us-east-1"
    dynamodb_table = "realty-etl-state-lock"
  }
}

provider "aws" {
  # access_key = var.aws_access_key_id
  # secret_key = var.aws_secret_access_key
  region = var.aws_region
}
