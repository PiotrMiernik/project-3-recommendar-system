resources "aws_s3_bucket" "data_lake" {
    bucket = var.s3_bucket_name
}

# Enforce bucket owner to own all objects in the bucket
resource "aws_s3_bucket_ownership_controls" "data_lake" {
    bucket = aws_s3_bucket.data_lake.id
    
    rule {
        object_ownership = "BucketOwnerEnforced"
    }
}

# Block all public access to the bucket
resource "aws_s3_bucket_public_access_block" "data_lake" {
    bucket                  = aws_s3_bucket.data_lake.id
    block_public_acls       = true
    block_public_policy     = true
    ignore_public_acls      = true
    restrict_public_buckets = true
}

# Default encryption for the bucket
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
    bucket = aws_s3_bucket.data_lake.id

    rule {
        apply_server_side_encryption_by_default {
            sse_algorithm = "AES256"
        }           
    }
}

# Lifecycle: control costs of noncurrent versions
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
    bucket = aws_s3_bucket.data_lake.id

    rule {
        id = "expire-noncurrent-versions"
        status = "Enabled"

        noncurrent_version_expiration {
            noncurrent_days = 30
        }
    }
}

# Deny insecure data transport
data "aws_iam_policy_document" "data_lake_bucket_policy" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    resources = [
      aws_s3_bucket.data_lake.arn,
      "${aws_s3_bucket.data_lake.arn}/*"
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  policy = data.aws_iam_policy_document.data_lake_bucket_policy.json
}