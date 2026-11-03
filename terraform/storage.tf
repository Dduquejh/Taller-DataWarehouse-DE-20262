# Bucket privado de extracciones.
#
#   s3://<bucket>/raw/<fuente>/<entidad>/batch_id=<id>/   archivos del lote
#   s3://<bucket>/control/                               registro de ejecuciones
#
# El versionado, junto con la política IAM sin s3:DeleteObject sobre raw/,
# impide que un lote se pierda o se sobrescriba en silencio: una escritura
# repetida deja la versión anterior recuperable.

resource "aws_s3_bucket" "dw" {
  bucket        = local.bucket_name
  force_destroy = var.s3_force_destroy
}

resource "aws_s3_bucket_public_access_block" "dw" {
  bucket = aws_s3_bucket.dw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "dw" {
  bucket = aws_s3_bucket.dw.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# SSE-S3: el COPY de Redshift lo descifra sin permisos adicionales de KMS.
resource "aws_s3_bucket_server_side_encryption_configuration" "dw" {
  bucket = aws_s3_bucket.dw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "dw" {
  bucket = aws_s3_bucket.dw.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "dw" {
  bucket = aws_s3_bucket.dw.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  depends_on = [aws_s3_bucket_versioning.dw]
}

# Rechaza cualquier acceso sin TLS.
data "aws_iam_policy_document" "bucket" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.dw.arn,
      "${aws_s3_bucket.dw.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "dw" {
  bucket = aws_s3_bucket.dw.id
  policy = data.aws_iam_policy_document.bucket.json

  depends_on = [aws_s3_bucket_public_access_block.dw]
}
