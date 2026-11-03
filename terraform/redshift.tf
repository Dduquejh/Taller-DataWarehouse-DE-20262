# Redshift Serverless: reemplaza a DuckDB.
#
# - Privado (publicly_accessible = false), en las tres subredes privadas.
# - enhanced_vpc_routing: el COPY desde S3 viaja por el endpoint gateway de
#   la VPC y no por internet.
# - El rol redshift_s3 autoriza el COPY (IAM_ROLE default en el SQL).
# - La contraseña del administrador la gestiona Secrets Manager.
# - Un límite diario de horas-RPU desactiva el workgroup si se excede.

resource "aws_redshiftserverless_namespace" "dw" {
  namespace_name = var.project_name
  db_name        = var.redshift_database

  admin_username        = var.redshift_admin_username
  manage_admin_password = true

  iam_roles            = [aws_iam_role.redshift_s3.arn]
  default_iam_role_arn = aws_iam_role.redshift_s3.arn

  log_exports = ["userlog", "connectionlog"]
}

resource "aws_redshiftserverless_workgroup" "dw" {
  workgroup_name = var.project_name
  namespace_name = aws_redshiftserverless_namespace.dw.namespace_name

  base_capacity = var.redshift_base_capacity
  max_capacity  = var.redshift_max_capacity

  subnet_ids           = aws_subnet.private[*].id
  security_group_ids   = [aws_security_group.redshift.id]
  publicly_accessible  = false
  enhanced_vpc_routing = true
  port                 = 5439

  # Corta consultas desbocadas (segundos).
  config_parameter {
    parameter_key   = "max_query_execution_time"
    parameter_value = "1800"
  }

  # Exige TLS en las conexiones de dbt y QuickSight.
  config_parameter {
    parameter_key   = "require_ssl"
    parameter_value = "true"
  }
}

resource "aws_redshiftserverless_usage_limit" "daily_compute" {
  resource_arn  = aws_redshiftserverless_workgroup.dw.arn
  usage_type    = "serverless-compute"
  amount        = var.redshift_daily_rpu_hours_limit
  period        = "daily"
  breach_action = "deactivate"
}
