# Assumptions:
# - Airflow runs locally (self-hosted) and assumes an AWS IAM role
# - EMR Serverless will be used for Spark transformations
# - Single S3 bucket with prefixes: raw/, staging/, mlready/
# - Principle of least privilege is applied
# - EMR Serverless uses a service-linked role managed by AWS
# - We create a separate runtime role for EMR Serverless jobs

data "aws_caller_identity" "current" {}


# TRUST POLICY: Allows EMR Serverless service to assume the runtime role
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

# S3 PERMISSIONS: Access to raw, staging, mlready, and logs
data "aws_iam_policy_document" "s3_emr_serverless_runtime" {
  statement {
    sid       = "ListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.s3_bucket_name}"]
  }

  statement {
    sid       = "ReadRawData"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:GetObjectTagging"]
    resources = ["arn:aws:s3:::${var.s3_bucket_name}/raw/*"]
  }

  # Permission to write to specified layers and save Spark logs
  statement {
    sid    = "WriteStagingMlreadyLogs"
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
      "arn:aws:s3:::${var.s3_bucket_name}/mlready/*",
      "arn:aws:s3:::${var.s3_bucket_name}/logs/*"
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