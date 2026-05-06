# EMR Serverless applications for Project 3
#
# The project uses two separate EMR Serverless applications:
#
# 1. Standard Spark application
#    - Used for regular PySpark ETL and feature engineering jobs.
#    - Uses the default EMR Serverless runtime image.
#    - Uses src_package.zip passed through --py-files.
#
# 2. Embeddings Spark application
#    - Used only for the review embeddings generation job.
#    - Uses a custom Docker image stored in Amazon ECR.
#    - The Docker image contains the ML/NLP dependencies required by
#      sentence-transformers and the embeddings generation script.
#
# Both applications use auto-start and auto-stop.
# No pre-initialized capacity is configured to avoid paying for idle workers.

resource "aws_emrserverless_application" "spark" {
  name           = "${var.project_name}-spark-serverless"
  release_label  = "emr-7.10.0"
  type           = "SPARK"
  architecture   = "X86_64"

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  maximum_capacity {
    cpu    = "20 vCPU"
    memory = "80 GB"
    disk   = "300 GB"
  }

  monitoring_configuration {
    s3_monitoring_configuration {
      log_uri = "s3://${aws_s3_bucket.data_lake.bucket}/logs/emr-serverless/"
    }
  }

  tags = {
    Project   = var.project_name
    Component = "emr-serverless-standard-spark"
  }
}

resource "aws_emrserverless_application" "embeddings" {
  name           = "${var.project_name}-embeddings-serverless"
  release_label  = "emr-7.10.0"
  type           = "SPARK"
  architecture   = "X86_64"

  image_configuration {
    image_uri = var.embeddings_ecr_image_uri
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 1
  }

  maximum_capacity {
    cpu    = "12 vCPU"
    memory = "48 GB"
    disk   = "200 GB"
  }

  monitoring_configuration {
    s3_monitoring_configuration {
      log_uri = "s3://${aws_s3_bucket.data_lake.bucket}/logs/emr-serverless/"
    }
  }

  tags = {
    Project   = var.project_name
    Component = "emr-serverless-embeddings"
  }
}