# This configuration follows the Principle of Least Privilege (PoLP) and sets up:
# 1. Orchestrator Role: Used by local Airflow to manage AWS resources.
# 2. EMR Serverless Runtime Role: Used by the Spark engine during execution.

data "aws_caller_identity" "current" {}

# 1. ORCHESTRATOR ROLE (For Local Airflow)

data "aws_iam_policy_document" "orchestrator_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }
}

resource "aws_iam_role" "orchestrator_role" {
  name               = "project3-orchestrator-role"
  assume_role_policy = data.aws_iam_policy_document.orchestrator_assume.json
}

data "aws_iam_policy_document" "orchestrator_policy_doc" {
  # Manage EMR Serverless Jobs
  statement {
    sid    = "EMRServerlessManagement"
    effect = "Allow"
    actions = [
      "emr-serverless:GetApplication",
      "emr-serverless:ListApplications",
      "emr-serverless:StartApplication",
      "emr-serverless:StopApplication",
      "emr-serverless:StartJobRun",
      "emr-serverless:GetJobRun",
      "emr-serverless:CancelJobRun",
      "emr-serverless:ListJobRuns"
    ]
    resources = ["*"]
  }

  # Required to attach the Runtime Role to the EMR Job
  statement {
    sid    = "IamPassRole"
    effect = "Allow"
    actions = ["iam:PassRole"]
    resources = [aws_iam_role.emr_serverless_runtime_role.arn]
    condition {
      test     = "StringLike"
      variable = "iam:PassedToService"
      values   = ["emr-serverless.amazonaws.com"]
    }
  }

  # Access for local ingestion scripts and GE validation
  statement {
    sid    = "S3AccessForAirflow"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject"
    ]
    resources = [
      "arn:aws:s3:::${var.s3_bucket_name}",
      "arn:aws:s3:::${var.s3_bucket_name}/*"
    ]
  }
}

resource "aws_iam_policy" "orchestrator_policy" {
  name   = "project3-orchestrator-policy"
  policy = data.aws_iam_policy_document.orchestrator_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "orchestrator_attach" {
  role       = aws_iam_role.orchestrator_role.name
  policy_arn = aws_iam_policy.orchestrator_policy.arn
}

# 2. EMR SERVERLESS RUNTIME ROLE (For Spark Execution)

data "aws_iam_policy_document" "emr_serverless_runtime_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_serverless_runtime_role" {
  name               = "project3-emr-serverless-runtime-role"
  assume_role_policy = data.aws_iam_policy_document.emr_serverless_runtime_assume.json
}

data "aws_iam_policy_document" "s3_emr_serverless_runtime" {
  # List Bucket for S3 connectivity
  statement {
    sid       = "ListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.s3_bucket_name}"]
  }

  # Read access for Raw, Staging, Spark Scripts and embedding libraries
  statement {
    sid       = "ReadInputAndScripts"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:GetObjectTagging"]
    resources = [
      "arn:aws:s3:::${var.s3_bucket_name}/raw/*",
      "arn:aws:s3:::${var.s3_bucket_name}/staging/*",
      "arn:aws:s3:::${var.s3_bucket_name}/jobs/*",
      "arn:aws:s3:::${var.s3_bucket_name}/artifacts/*",
    ]
  }

  # Write access for Staging, MLReady and Logs
  # Note: GetObject is also needed for MLReady to handle Iceberg metadata operations
  statement {
    sid    = "WriteOutputAndLogs"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:PutObjectTagging",
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]
    resources = [
      "arn:aws:s3:::${var.s3_bucket_name}/staging/*",
      "arn:aws:s3:::${var.s3_bucket_name}/mlready/*",
      "arn:aws:s3:::${var.s3_bucket_name}/logs/*"
    ]
  }

  # Glue Catalog Permissions for Apache Iceberg
  statement {
    sid    = "GlueCatalogAccess"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:BatchGetPartition"
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/mlready",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/mlready/*"
    ]
  }
}

resource "aws_iam_policy" "s3_emr_serverless_runtime" {
  name   = "project3-s3-emr-serverless-runtime-policy"
  policy = data.aws_iam_policy_document.s3_emr_serverless_runtime.json
}

resource "aws_iam_role_policy_attachment" "emr_serverless_runtime_s3_attach" {
  role       = aws_iam_role.emr_serverless_runtime_role.name
  policy_arn = aws_iam_policy.s3_emr_serverless_runtime.arn
}