# Assumptions:
# - This project reuses an existing RDS PostgreSQL instance instead of creating a new one
# - The existing instance is treated as an external dependency
# - Source DB already exists ("recommender")
# - Vector DB for embeddings will be created later logically inside the same RDS instance
#   (for example as a separate database or schema)
# - Terraform does not manage the lifecycle of the existing RDS instance

data "aws_db_instance" "existing" {
  db_instance_identifier = var.existing_rds_instance_identifier
}