# Assumptions:
# - Airflow runs locally (self-hosted) and assumes an AWS IAM role
# - EMR Serverless will be used for Spark transformations
# - Single S3 bucket with prefixes: raw/, staging/, mlready/
# - Principle of least privilege is applied
# - EMR Serverless uses a service-linked role managed by AWS
# - We create a separate runtime role for EMR Serverless jobs

data "aws_caller_identity" "current" {}


# EMR SERVERLESS RUNTIME ROLE

# This trust policy allows EMR Serverless to assume the runtime role
# that will be used by Spark jobs during execution.
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

# This policy grants EMR Serverless jobs access to the project S3 bucket.
# Jobs can read from raw and write to staging and mlready.
data "aws_iam_policy_document" "s3_emr_serverless_runtime" {

  # Allow listing only the relevant project prefixes
  statement {
    sid       = "ListBucketForRelevantPrefixes"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.s3_bucket_name}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*", "staging/*", "mlready/*"]
    }
  }

  # Allow reading source data from the raw layer
  statement {
    sid       = "ReadRaw"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:GetObjectTagging"]
    resources = ["arn:aws:s3:::${var.s3_bucket_name}/raw/*"]
  }

  # Allow writing transformed data to staging and mlready
  statement {
    sid    = "WriteStagingMlready"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:PutObjectTagging",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]
    resources = [
      "arn:aws:s3:::${var.s3_bucket_name}/staging/*",
      "arn:aws:s3:::${var.s3_bucket_name}/mlready/*"
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


# ORCHESTRATOR ROLE (ASSUMED BY LOCAL AIRFLOW)

# This role is assumed by the current AWS account and used by local Airflow
# to orchestrate S3 ingestion and EMR Serverless application/job execution.
data "aws_iam_policy_document" "orchestrator_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [
        "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
      ]
    }
  }
}

resource "aws_iam_role" "orchestrator_role" {
  name               = "project3-orchestrator-role"
  assume_role_policy = data.aws_iam_policy_document.orchestrator_assume.json
}

# This policy grants Airflow:
# - S3 access for ingestion and orchestration
# - permissions to create and manage EMR Serverless applications and job runs
# - permission to pass the EMR Serverless runtime role
# - permission to create the EMR Serverless service-linked role if needed
data "aws_iam_policy_document" "orchestrator_permissions" {

  # Allow orchestration-level access to all project data lake layers
  statement {
    sid    = "S3Access"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetObject",
      "s3:GetObjectTagging",
      "s3:PutObject",
      "s3:PutObjectTagging",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]
    resources = [
      "arn:aws:s3:::${var.s3_bucket_name}",
      "arn:aws:s3:::${var.s3_bucket_name}/*"
    ]
  }

  # Allow Airflow to manage EMR Serverless applications and job runs
  statement {
    sid    = "EMRServerlessControlPlane"
    effect = "Allow"
    actions = [
      "emr-serverless:CreateApplication",
      "emr-serverless:GetApplication",
      "emr-serverless:ListApplications",
      "emr-serverless:StartApplication",
      "emr-serverless:StopApplication",
      "emr-serverless:DeleteApplication",
      "emr-serverless:StartJobRun",
      "emr-serverless:GetJobRun",
      "emr-serverless:ListJobRuns",
      "emr-serverless:CancelJobRun",
      "emr-serverless:TagResource",
      "emr-serverless:UntagResource",
      "emr-serverless:ListTagsForResource"
    ]
    resources = ["*"]
  }

  # Allow passing the EMR Serverless runtime role to the service
  statement {
    sid     = "PassRuntimeRoleToEMRServerless"
    effect  = "Allow"
    actions = ["iam:PassRole"]
    resources = [
      aws_iam_role.emr_serverless_runtime_role.arn
    ]
  }

  # Allow creation of the EMR Serverless service-linked role if it does not exist yet
  statement {
    sid     = "CreateEMRServerlessServiceLinkedRole"
    effect  = "Allow"
    actions = ["iam:CreateServiceLinkedRole"]
    resources = [
      "arn:aws:iam::*:role/aws-service-role/ops.emr-serverless.amazonaws.com/AWSServiceRoleForAmazonEMRServerless*"
    ]

    condition {
      test     = "StringLike"
      variable = "iam:AWSServiceName"
      values   = ["ops.emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_policy" "orchestrator_permissions" {
  name   = "project3-orchestrator-permissions"
  policy = data.aws_iam_policy_document.orchestrator_permissions.json
}

resource "aws_iam_role_policy_attachment" "orchestrator_permissions_attach" {
  role       = aws_iam_role.orchestrator_role.name
  policy_arn = aws_iam_policy.orchestrator_permissions.arn
}