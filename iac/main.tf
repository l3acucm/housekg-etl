terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.82"
    }
  }

  backend "s3" {
    bucket = "hello-data-terraform-backend"
    key    = "mvp.tfstate"
    region = "us-east-1"
    dynamodb_table = "data-infrastructure-state-lock"
  }
}

provider "aws" {
  # access_key = var.aws_access_key_id
  # secret_key = var.aws_secret_access_key
  region = var.aws_region
}
