# Purpose:
# - Provide the key infrastructure values after `terraform apply`
# - Make it easy to populate config/.env for local Airflow and scripts



# S3
output "s3_bucket_name" {
  description = "Main S3 data lake bucket name (prefixes: raw/, silver/, gold/)"
  value       = aws_s3_bucket.data_lake.bucket
}


# RDS PostgreSQL
output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint (hostname)"
  value       = aws_db_instance.postgres.address
}

output "rds_port" {
  description = "RDS PostgreSQL port"
  value       = aws_db_instance.postgres.port
}

output "rds_db_name" {
  description = "RDS database name"
  value       = aws_db_instance.postgres.db_name
}

output "rds_username" {
  description = "RDS master username"
  value       = aws_db_instance.postgres.username
}


# IAM (Orchestrator for local Airflow)
output "orchestrator_role_arn" {
  description = "IAM role ARN to be assumed by local Airflow (STS AssumeRole)"
  value       = aws_iam_role.orchestrator_role.arn
}


# IAM (EMR roles)
output "emr_service_role_arn" {
  description = "EMR service role ARN (used by EMR control plane)"
  value       = aws_iam_role.emr_service_role.arn
}

output "emr_ec2_role_arn" {
  description = "EMR EC2 role ARN (used by EC2 instances in the EMR cluster)"
  value       = aws_iam_role.emr_ec2_role.arn
}

output "emr_ec2_instance_profile_name" {
  description = "EMR EC2 instance profile name (to attach to EMR cluster instances)"
  value       = aws_iam_instance_profile.emr_ec2_instance_profile.name
}
