# ---------------------------------------------------------------- Recursos

output "bucket_name" {
  description = "Bucket privado de extracciones"
  value       = aws_s3_bucket.dw.bucket
}

output "mysql_endpoint" {
  description = "Endpoint privado de RDS MySQL"
  value       = aws_db_instance.mysql.address
}

output "docdb_endpoint" {
  description = "Endpoint privado del cluster DocumentDB"
  value       = aws_docdb_cluster.docdb.endpoint
}

output "redshift_endpoint" {
  description = "Endpoint privado del workgroup de Redshift Serverless"
  value       = aws_redshiftserverless_workgroup.dw.endpoint[0].address
}

output "redshift_copy_role_arn" {
  description = "Rol IAM que usa COPY (también es el rol por defecto del namespace)"
  value       = aws_iam_role.redshift_s3.arn
}

output "secret_arns" {
  description = "Secretos con las credenciales de cada base (gestionados por AWS)"
  value = {
    mysql    = aws_db_instance.mysql.master_user_secret[0].secret_arn
    docdb    = aws_docdb_cluster.docdb.master_user_secret[0].secret_arn
    redshift = aws_redshiftserverless_namespace.dw.admin_password_secret_arn
  }
}

output "pipeline_config_parameter" {
  description = "Parámetro de SSM con la configuración no secreta (.env)"
  value       = aws_ssm_parameter.pipeline_env.name
}

output "quicksight_vpc_connection_arn" {
  description = "Conexión VPC de QuickSight (si está habilitada)"
  value       = one(aws_quicksight_vpc_connection.redshift[*].arn)
}

# ---------------------------------------------------------------- Instancia

output "instance_id" {
  description = "ID de la instancia de Airflow"
  value       = aws_instance.airflow.id
}

output "public_ip" {
  description = "IP pública actual (cambia en cada stop/start)"
  value       = aws_instance.airflow.public_ip
}

output "airflow_url" {
  description = "URL del UI de Airflow"
  value       = length(var.allowed_cidrs) > 0 ? "http://${aws_instance.airflow.public_ip}:8080" : "http://localhost:8080 (con el túnel ssm_tunnel_airflow)"
}

output "ssm_session" {
  description = "Terminal en la instancia por Session Manager"
  value       = "aws ssm start-session --target ${aws_instance.airflow.id} --region ${var.aws_region}${local.profile_flag}"
}

# ---------------------------------------------------------------- Túneles
#
# Las bases son privadas. Para DBeaver, Compass o el editor de Redshift
# desde el equipo local se abre un túnel por Session Manager a través de la
# instancia; no hay puertos abiertos a internet.

output "ssm_tunnel_airflow" {
  description = "Túnel al UI de Airflow en localhost:8080"
  value       = "aws ssm start-session --target ${aws_instance.airflow.id} --region ${var.aws_region}${local.profile_flag} --document-name AWS-StartPortForwardingSession --parameters '{\"portNumber\":[\"8080\"],\"localPortNumber\":[\"8080\"]}'"
}

output "ssm_tunnel_mysql" {
  description = "Túnel a RDS MySQL en localhost:3307"
  value       = "aws ssm start-session --target ${aws_instance.airflow.id} --region ${var.aws_region}${local.profile_flag} --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters '{\"host\":[\"${aws_db_instance.mysql.address}\"],\"portNumber\":[\"3306\"],\"localPortNumber\":[\"3307\"]}'"
}

output "ssm_tunnel_docdb" {
  description = "Túnel a DocumentDB en localhost:27018 (usar tlsAllowInvalidHostnames=true)"
  value       = "aws ssm start-session --target ${aws_instance.airflow.id} --region ${var.aws_region}${local.profile_flag} --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters '{\"host\":[\"${aws_docdb_cluster.docdb.endpoint}\"],\"portNumber\":[\"27017\"],\"localPortNumber\":[\"27018\"]}'"
}

output "ssm_tunnel_redshift" {
  description = "Túnel a Redshift en localhost:5439"
  value       = "aws ssm start-session --target ${aws_instance.airflow.id} --region ${var.aws_region}${local.profile_flag} --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters '{\"host\":[\"${aws_redshiftserverless_workgroup.dw.endpoint[0].address}\"],\"portNumber\":[\"5439\"],\"localPortNumber\":[\"5439\"]}'"
}

# ---------------------------------------------------------------- Ayuda

output "secret_value_commands" {
  description = "Comandos para leer las credenciales gestionadas (solo para depurar)"
  value = {
    for name, arn in {
      mysql    = aws_db_instance.mysql.master_user_secret[0].secret_arn
      docdb    = aws_docdb_cluster.docdb.master_user_secret[0].secret_arn
      redshift = aws_redshiftserverless_namespace.dw.admin_password_secret_arn
    } :
    name => "aws secretsmanager get-secret-value --secret-id '${arn}' --region ${var.aws_region}${local.profile_flag} --query SecretString --output text"
  }
}

output "airflow_password_command" {
  description = "Contraseña del usuario airflow (dentro de la sesión SSM)"
  value       = "sudo grep AIRFLOW_ADMIN_PASSWORD ${local.app_dir}/.airflow-secrets"
}
