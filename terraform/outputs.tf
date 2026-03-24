# Purpose:
# - Provide the key infrastructure values after `terraform apply`
# - Make it easy to populate config/.env for local Airflow and scripts

# S3
output "s3_bucket_name" {
  description = "Main S3 data lake bucket name (prefixes: raw/, silver/, gold/)"
  value       = aws_s3_bucket.data_lake.bucket
}


# RDS PostgreSQL (endpoint and port for existing RDS instance)
output "existing_rds_endpoint" {
  value = data.aws_db_instance.existing.address
}

output "existing_rds_port" {
  value = data.aws_db_instance.existing.port
}


# IAM (Orchestrator for local Airflow)
output "orchestrator_role_arn" {
  description = "IAM role ARN to be assumed by local Airflow (STS AssumeRole)"
  value       = aws_iam_role.orchestrator_role.arn
}


# IAM (EMR Serverless)
output "emr_serverless_runtime_role_arn" {
  description = "IAM role ARN used by EMR Serverless jobs (runtime role)"
  value       = aws_iam_role.emr_serverless_runtime_role.arn
}
