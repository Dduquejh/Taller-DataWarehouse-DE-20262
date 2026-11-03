# Instancia de Airflow y dbt.
#
# La configuración no secreta (endpoints, bucket, ARNs de los secretos) va a
# un parámetro de SSM. user_data la convierte en .env al arrancar y deja el
# comando dw-render-env para regenerarlo si algo cambia (p. ej. tras recrear
# una base), sin reemplazar la instancia.

resource "aws_ssm_parameter" "pipeline_env" {
  name        = local.config_parameter_name
  description = "Configuración no secreta del pipeline (formato .env)"
  type        = "String"
  tier        = "Standard"

  value = join("\n", [
    "AWS_REGION=${var.aws_region}",
    "AWS_DEFAULT_REGION=${var.aws_region}",
    "S3_BUCKET=${aws_s3_bucket.dw.bucket}",
    "S3_RAW_PREFIX=${trimsuffix(local.raw_prefix, "/")}",
    "S3_CONTROL_PREFIX=${trimsuffix(local.control_prefix, "/")}",
    "TLS_CA_FILE=/opt/airflow/certs/global-bundle.pem",
    "MYSQL_HOST=${aws_db_instance.mysql.address}",
    "MYSQL_PORT=${aws_db_instance.mysql.port}",
    "MYSQL_DATABASE=${var.mysql_database}",
    "MYSQL_SECRET_ARN=${aws_db_instance.mysql.master_user_secret[0].secret_arn}",
    "DOCDB_HOST=${aws_docdb_cluster.docdb.endpoint}",
    "DOCDB_PORT=${aws_docdb_cluster.docdb.port}",
    "MONGO_DATABASE=${var.mongo_database}",
    "DOCDB_SECRET_ARN=${aws_docdb_cluster.docdb.master_user_secret[0].secret_arn}",
    "REDSHIFT_HOST=${aws_redshiftserverless_workgroup.dw.endpoint[0].address}",
    "REDSHIFT_PORT=${aws_redshiftserverless_workgroup.dw.endpoint[0].port}",
    "REDSHIFT_DATABASE=${var.redshift_database}",
    "REDSHIFT_WORKGROUP=${aws_redshiftserverless_workgroup.dw.workgroup_name}",
    "REDSHIFT_SECRET_ARN=${aws_redshiftserverless_namespace.dw.admin_password_secret_arn}",
    "REDSHIFT_COPY_ROLE_ARN=${aws_iam_role.redshift_s3.arn}",
    "QUICKSIGHT_DATASET_IDS=${join(",", var.quicksight_dataset_ids)}",
  ])
}

data "aws_ssm_parameter" "al2023_ami" {
  name = var.ami_ssm_parameter
}

resource "aws_instance" "airflow" {
  ami           = data.aws_ssm_parameter.al2023_ami.insecure_value
  instance_type = var.instance_type

  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.airflow.id]
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.airflow.name
  key_name                    = var.key_name

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    app_dir                = local.app_dir
    repo_url               = var.repo_url
    repo_branch            = var.repo_branch
    aws_region             = var.aws_region
    config_parameter_name  = aws_ssm_parameter.pipeline_env.name
    docker_compose_version = var.docker_compose_version
    docker_buildx_version  = var.docker_buildx_version
  })

  # IMDSv2 obligatorio. Hop limit 2 porque Airflow corre en contenedores con
  # red bridge: sin él boto3 no obtiene las credenciales del rol.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
    instance_metadata_tags      = "disabled"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_size
    encrypted             = true
    delete_on_termination = true
  }

  tags        = { Name = "${var.project_name}-airflow" }
  volume_tags = { Name = "${var.project_name}-airflow" }

  lifecycle {
    # Una AMI nueva o un cambio de user_data no recrean la instancia (se
    # perdería el Postgres de Airflow). Para aplicarlos:
    #   terraform apply -replace=aws_instance.airflow
    ignore_changes = [ami, user_data]
  }

  # Las bases existen antes del arranque, aunque Airflow no las necesita
  # hasta la primera ejecución del DAG.
  depends_on = [
    aws_iam_role_policy.airflow,
    aws_db_instance.mysql,
    aws_docdb_cluster_instance.docdb,
  ]
}
