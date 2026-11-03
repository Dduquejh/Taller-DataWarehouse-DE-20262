# Roles y permisos del ambiente. Ningún componente usa llaves de acceso:
#
# | Rol                       | Lo asume            | Para                                   |
# |---------------------------|---------------------|----------------------------------------|
# | <proyecto>-airflow-ec2    | EC2 (Airflow + dbt) | S3 raw/control, secretos, Redshift, SPICE |
# | <proyecto>-redshift-s3    | Redshift            | COPY de raw/ (solo lectura)            |
# | <proyecto>-quicksight-vpc | QuickSight          | Crear sus interfaces en la VPC         |

# ================================================================ EC2 (Airflow)

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "airflow" {
  name               = "${var.project_name}-airflow-ec2"
  description        = "Instancia EC2 que ejecuta Airflow y dbt"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

data "aws_iam_policy_document" "airflow" {
  # ---- S3
  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket", "s3:ListBucketVersions", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.dw.arn]
  }

  # Sin DeleteObject en raw/: los lotes extraídos son inmutables y el
  # reprocesamiento depende de ellos.
  statement {
    sid = "ReadWriteRaw"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.dw.arn}/${local.raw_prefix}*"]
  }

  statement {
    sid = "ReadWriteControl"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = ["${aws_s3_bucket.dw.arn}/${local.control_prefix}*"]
  }

  # ---- Secretos de las tres bases (contraseñas gestionadas por AWS)
  statement {
    sid     = "ReadDatabaseSecrets"
    actions = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
    resources = [
      aws_db_instance.mysql.master_user_secret[0].secret_arn,
      aws_docdb_cluster.docdb.master_user_secret[0].secret_arn,
      aws_redshiftserverless_namespace.dw.admin_password_secret_arn,
    ]
  }

  # ---- Configuración no secreta del pipeline
  statement {
    sid       = "ReadPipelineConfig"
    actions   = ["ssm:GetParameter", "ssm:GetParameters"]
    resources = [aws_ssm_parameter.pipeline_env.arn]
  }

  # ---- Redshift: credenciales IAM temporales y Data API como alternativa
  statement {
    sid = "RedshiftWorkgroup"
    actions = [
      "redshift-serverless:GetCredentials",
      "redshift-serverless:GetWorkgroup",
      "redshift-serverless:GetNamespace",
    ]
    resources = [
      aws_redshiftserverless_workgroup.dw.arn,
      aws_redshiftserverless_namespace.dw.arn,
    ]
  }

  statement {
    sid       = "RedshiftDataApiExecute"
    actions   = ["redshift-data:ExecuteStatement", "redshift-data:BatchExecuteStatement"]
    resources = [aws_redshiftserverless_workgroup.dw.arn]
  }

  # Describe/GetStatementResult no admiten ARN de recurso; la Data API
  # solo deja ver las sentencias del propio rol.
  statement {
    sid = "RedshiftDataApiResults"
    actions = [
      "redshift-data:DescribeStatement",
      "redshift-data:GetStatementResult",
      "redshift-data:CancelStatement",
      "redshift-data:ListStatements",
    ]
    resources = ["*"]
  }

  # ---- QuickSight: refrescar datasets SPICE al terminar un lote
  statement {
    sid = "RefreshSpice"
    actions = [
      "quicksight:CreateIngestion",
      "quicksight:DescribeIngestion",
      "quicksight:ListIngestions",
    ]
    resources = ["arn:${data.aws_partition.current.partition}:quicksight:${var.aws_region}:${local.account_id}:dataset/*"]
  }
}

resource "aws_iam_role_policy" "airflow" {
  name   = "${var.project_name}-pipeline"
  role   = aws_iam_role.airflow.id
  policy = data.aws_iam_policy_document.airflow.json
}

# Session Manager: terminal y túneles sin abrir puertos ni repartir llaves.
resource "aws_iam_role_policy_attachment" "airflow_ssm" {
  role       = aws_iam_role.airflow.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "airflow" {
  name = "${var.project_name}-airflow-ec2"
  role = aws_iam_role.airflow.name
}

# ================================================================ Redshift → S3

data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "redshift_s3" {
  name               = "${var.project_name}-redshift-s3"
  description        = "Redshift lee los lotes de raw/ con COPY"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
}

data "aws_iam_policy_document" "redshift_s3" {
  statement {
    sid       = "ListRaw"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.dw.arn]
  }

  statement {
    sid       = "ReadRaw"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.dw.arn}/${local.raw_prefix}*"]
  }
}

resource "aws_iam_role_policy" "redshift_s3" {
  name   = "${var.project_name}-copy-from-raw"
  role   = aws_iam_role.redshift_s3.id
  policy = data.aws_iam_policy_document.redshift_s3.json
}

# ================================================================ QuickSight VPC

data "aws_iam_policy_document" "quicksight_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["quicksight.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "quicksight_vpc" {
  count = var.enable_quicksight_vpc_connection ? 1 : 0

  name               = "${var.project_name}-quicksight-vpc"
  description        = "QuickSight crea sus interfaces de red en la VPC"
  assume_role_policy = data.aws_iam_policy_document.quicksight_assume.json
}

data "aws_iam_policy_document" "quicksight_vpc" {
  statement {
    actions = [
      "ec2:CreateNetworkInterface",
      "ec2:ModifyNetworkInterfaceAttribute",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeSubnets",
      "ec2:DescribeSecurityGroups",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "quicksight_vpc" {
  count = var.enable_quicksight_vpc_connection ? 1 : 0

  name   = "${var.project_name}-quicksight-eni"
  role   = aws_iam_role.quicksight_vpc[0].id
  policy = data.aws_iam_policy_document.quicksight_vpc.json
}
