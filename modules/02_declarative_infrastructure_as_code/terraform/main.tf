# Production Lakehouse Storage & IAM Declarative Definition
terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

resource "aws_s3_bucket" "lakehouse_storage" {
  bucket        = "enterprise-lakehouse-production-storage"
  force_destroy = false

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse_encryption" {
  bucket = aws_s3_bucket.lakehouse_storage.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}
