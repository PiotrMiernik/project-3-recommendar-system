# EMR Serverless for Spark transformations
# Assumptions:
# - Lowest-cost setup for Project 3
# - Airflow runs locally and submits jobs to EMR Serverless
# - One Spark application reused across jobs
# - Auto-start enabled
# - Auto-stop enabled
# - No pre-initialized capacity (to avoid paying for warm workers)
# - Runtime role for EMR Serverless jobs is created in iam.tf

resource "aws_emrserverless_application" "spark" {
  name          = "${var.project_name}-spark-serverless"
  release_label = "emr-7.10.0"
  type          = "SPARK"
  architecture  = "X86_64"

  # Minimal settings to keep costs low
  auto_start_configuration {
    enabled = true
  }

  # Shutdown application after 1 minute of inactivity to save costs
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 1 
  }

  # Conservative resource limits for initial runs
  maximum_capacity {
    cpu    = "20 vCPU"
    memory = "80 GB"
    disk   = "200 GB"
  }

  # Enable S3 logging for job debugging
  monitoring_configuration {
    s3_monitoring_configuration {
      log_uri = "s3://${aws_s3_bucket.data_lake.bucket}/logs/emr-serverless/"
    }
  }

  tags = {
    Project   = var.project_name
    Component = "emr-serverless"
  }
}