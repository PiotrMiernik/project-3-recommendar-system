# Global / project settings

variable "aws_region" {
  description = "AWS region where resources will be deployed"
  type        = string
}

variable "project_name" {
  description = "Logical name of the project (used for tagging and naming resources)"
  type        = string
}

# S3 / Data Lake

variable "s3_bucket_name" {
  description = "Name of the s3 main bucket used for the data lake (raw/silver/gold prefixes)"
  type        = string
}

# RDS / PostgreSQL

variable "existing_rds_instance_identifier" {
  description = "Identifier of the existing RDS PostgreSQL instance used by the project"
  type        = string
}

# ECR / EMR

variable "embeddings_ecr_image_uri" {
  description = "ECR image URI for the EMR Serverless embeddings application"
  type        = string
}