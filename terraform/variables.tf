# Global / project settings

variable "aws_region" {
    description = "AWS region where resources will be deployed"
    type        = string
}

variable "project_name" {
    description = "Logical name of the project (used for tagging and naming resources)"
    type = string
}

# S3 / Data Lake

variable "s3_bucket_name" {
    description = "Name of the s3 main bucket used for the data lake (raw/silver/gold prefixes)"
    type = string
}

# RDS / PostgresSQL

variable "db_name" {
    description = "PostgresSQL database name"
    type = string
}

variable "db_username" {
    description = "Master username for PostgresSQL database"
    type = string
}

variable "db_password" {
    description = "Master password for PostgresSQL database"
    type = string
}

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t4g.micro"
}

variable "db_allocated_storage_gb" {
  description = "Allocated storage for RDS in GB"
  type        = number
  default     = 20
}

# Networking / Security

variable "allowed_cidr_blocks" {
    description = "List of CIDR blocks allowed to connect to RDS"
    type        = list(string)
    default     = []
}
