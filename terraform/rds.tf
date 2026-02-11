# Assumptions:
# - Cost-minimized educational project (small instance class, minimal storage)
# - Airflow runs locally, so DB access from your laptop is needed
# - I use the default VPC (Variant A) to keep infrastructure simple
# - RDS is publicly accessible BUT inbound access is restricted to allowed CIDRs
# - pgvector will be installed later inside PostgreSQL (it is not an AWS resource)


# Networking (default VPC)

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}


# DB subnet group

resource "aws_db_subnet_group" "this" {
  name       = "${var.project_name}-db-subnet-group"
  subnet_ids = data.aws_subnets.default.ids

  tags = {
    Name = "${var.project_name}-db-subnet-group"
  }
}


# Security group (restrict inbound to Postgres)

resource "aws_security_group" "rds" {
  name        = "${var.project_name}-rds-sg"
  description = "Allow PostgreSQL access only from allowed CIDR blocks"
  vpc_id      = data.aws_vpc.default.id

  # Inbound: PostgreSQL from allowed CIDRs only
  ingress {
    description = "PostgreSQL"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  # Outbound: allow all (typical default)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-rds-sg"
  }
}

# Optional: parameter group (kept minimal for Stage 1)

resource "aws_db_parameter_group" "postgres" {
  name   = "${var.project_name}-pg-params"
  family = "postgres15"

  tags = {
    Name = "${var.project_name}-pg-params"
  }
}


# RDS instance

resource "aws_db_instance" "postgres" {
  identifier = "${var.project_name}-postgres"

  engine         = "postgres"
  engine_version = "15.5"

  instance_class    = var.db_instance_class
  allocated_storage = var.db_allocated_storage_gb

  db_name  = var.db_name
  username = var.db_username
  password = var.db_password

  # Networking
  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  # Make it reachable from my laptop
  publicly_accessible = true

  # Storage / safety
  storage_encrypted    = true
  parameter_group_name = aws_db_parameter_group.postgres.name

  # Backups: keep small but non-zero (helps avoid accidental data loss)
  backup_retention_period = 7

  # Avoid high costs
  deletion_protection = false
  skip_final_snapshot = true

  tags = {
    Name = "${var.project_name}-postgres"
  }
}
