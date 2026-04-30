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

  # Shutdown application after 15 minute of inactivity to save costs
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15 
  }

  # Initial Capacity: Pre-provisions resources to ensure workers are ready immediately
  # and have specific hardware configurations (like large enough disks for ML environments)
  initial_capacity {
    initial_capacity_type = "Driver"
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "4 vCPU"
        memory = "16 GB"
        disk   = "100 GB"
      }
    }
  }

  initial_capacity {
    initial_capacity_type = "Executor"
    initial_capacity_config {
      worker_count = 2 # Two executors to process data in parallel
      worker_configuration {
        cpu    = "4 vCPU"
        memory = "16 GB"
        disk   = "100 GB"
      }
    }
  }

  # Maximum Capacity: The absolute ceiling for the entire application across all concurrent jobs
  # Prevents runaway costs if multiple jobs are triggered or if auto-scaling goes too far
  maximum_capacity {
    cpu    = "20 vCPU"
    memory = "80 GB"
    disk   = "300 GB"
  }

  # Monitoring: Routes all Spark stdout, stderr, and event logs to S3 for debugging
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