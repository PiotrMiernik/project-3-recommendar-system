# Assumptions:
# - Airflow runs locally (self-hosted) and assumes an AWS IAM role
# - EMR is used as a one-time job cluster (auto-terminate)
# - Single S3 bucket with prefixes: raw/, silver/, gold/
# - Principle of least privilege applied

data "aws_caller_identity" "current" {}

# S3 POLICY FOR ORCHESTRATOR (Airflow local)

# Allows full orchestration access to raw/silver/gold prefixes
data "aws_iam_policy_document" "s3_orchestrator" {

  # Allow listing only relevant prefixes
  statement {
    sid     = "ListBucketForRelevantPrefixes"
    effect  = "Allow"
    actions = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.s3_bucket_name}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*", "silver/*", "gold/*"]
    }
  }

  # Allow read/write/delete within raw, silver and gold
  statement {
    sid    = "ReadWriteObjectsInRawSilverGold"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectTagging",
      "s3:PutObject",
      "s3:PutObjectTagging",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]
    resources = [
      "arn:aws:s3:::${var.s3_bucket_name}/raw/*",
      "arn:aws:s3:::${var.s3_bucket_name}/silver/*",
      "arn:aws:s3:::${var.s3_bucket_name}/gold/*"
    ]
  }
}

resource "aws_iam_policy" "s3_orchestrator" {
  name   = "project3-s3-orchestrator-policy"
  policy = data.aws_iam_policy_document.s3_orchestrator.json
}


# S3 POLICY FOR EMR PROCESSING ROLE

# EMR EC2 instances read from raw and write to silver/gold
data "aws_iam_policy_document" "s3_emr_processing" {

  # Allow listing relevant prefixes
  statement {
    sid     = "ListBucketForRelevantPrefixes"
    effect  = "Allow"
    actions = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.s3_bucket_name}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*", "silver/*", "gold/*"]
    }
  }

  # Allow read from raw
  statement {
    sid     = "ReadRaw"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:GetObjectTagging"]
    resources = ["arn:aws:s3:::${var.s3_bucket_name}/raw/*"]
  }

  # Allow write to silver and gold
  statement {
    sid    = "WriteSilverGold"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:PutObjectTagging",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]
    resources = [
      "arn:aws:s3:::${var.s3_bucket_name}/silver/*",
      "arn:aws:s3:::${var.s3_bucket_name}/gold/*"
    ]
  }
}

resource "aws_iam_policy" "s3_emr_processing" {
  name   = "project3-s3-emr-processing-policy"
  policy = data.aws_iam_policy_document.s3_emr_processing.json
}


# EMR SERVICE ROLE (CONTROL PLANE)

# Role assumed by the EMR service itself
data "aws_iam_policy_document" "emr_service_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_service_role" {
  name               = "project3-emr-service-role"
  assume_role_policy = data.aws_iam_policy_document.emr_service_assume.json
}

# Attach AWS managed EMR service role policy
resource "aws_iam_role_policy_attachment" "emr_service_managed" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}


# EMR EC2 ROLE (DATA PLANE)

# Role assumed by EC2 instances in the EMR cluster
data "aws_iam_policy_document" "emr_ec2_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_ec2_role" {
  name               = "project3-emr-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.emr_ec2_assume.json
}

# Instance profile required by EMR
resource "aws_iam_instance_profile" "emr_ec2_instance_profile" {
  name = "project3-emr-ec2-instance-profile"
  role = aws_iam_role.emr_ec2_role.name
}

# Attach AWS managed baseline policy for EMR EC2
resource "aws_iam_role_policy_attachment" "emr_ec2_managed" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

# Attach custom S3 processing policy
resource "aws_iam_role_policy_attachment" "emr_ec2_s3_processing" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = aws_iam_policy.s3_emr_processing.arn
}


# ORCHESTRATOR ROLE (ASSUMED BY LOCAL AIRFLOW)

# Allow this AWS account to assume the orchestrator role
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

# Permissions required by Airflow to orchestrate EMR and S3
data "aws_iam_policy_document" "orchestrator_permissions" {

  # S3 access (orchestration level)
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

  # EMR cluster and step control
  statement {
    sid    = "EMRControlPlane"
    effect = "Allow"
    actions = [
      "elasticmapreduce:RunJobFlow",
      "elasticmapreduce:TerminateJobFlows",
      "elasticmapreduce:AddJobFlowSteps",
      "elasticmapreduce:DescribeCluster",
      "elasticmapreduce:DescribeStep",
      "elasticmapreduce:ListClusters",
      "elasticmapreduce:ListSteps",
      "elasticmapreduce:ListInstanceGroups",
      "elasticmapreduce:ListInstances"
    ]
    resources = ["*"]
  }

  # Read-only EC2 permissions required by EMR APIs
  statement {
    sid    = "EC2DescribeReadOnly"
    effect = "Allow"
    actions = [
      "ec2:DescribeInstances",
      "ec2:DescribeSubnets",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeVpcs",
      "ec2:DescribeRouteTables",
      "ec2:DescribeAvailabilityZones",
      "ec2:DescribeInstanceTypes"
    ]
    resources = ["*"]
  }

  # Required to pass EMR roles when creating cluster
  statement {
    sid     = "PassRoleForEMR"
    effect  = "Allow"
    actions = ["iam:PassRole"]
    resources = [
      aws_iam_role.emr_service_role.arn,
      aws_iam_role.emr_ec2_role.arn
    ]
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
