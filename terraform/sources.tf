# Fuentes operativas: RDS for MySQL y DocumentDB.
#
# Las contraseñas maestras las genera y guarda Secrets Manager
# (manage_master_user_password): no aparecen en el código, en .env ni en el
# state de Terraform. El pipeline las lee en tiempo de ejecución con el rol
# de la instancia.
#
# Ambas exigen TLS. El certificado raíz (global-bundle.pem) es el mismo para
# RDS y DocumentDB; user_data lo descarga en <repo>/certs/.

# ================================================================ RDS MySQL

resource "aws_db_subnet_group" "mysql" {
  name       = "${var.project_name}-mysql"
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_db_parameter_group" "mysql" {
  name        = "${var.project_name}-mysql84"
  family      = "mysql8.4"
  description = "MySQL del data warehouse: TLS obligatorio y utf8mb4"

  parameter {
    name  = "require_secure_transport"
    value = "1"
  }

  parameter {
    name  = "character_set_server"
    value = "utf8mb4"
  }

  parameter {
    name  = "collation_server"
    value = "utf8mb4_0900_ai_ci"
  }
}

resource "aws_db_instance" "mysql" {
  identifier     = "${var.project_name}-mysql"
  engine         = "mysql"
  engine_version = var.mysql_engine_version
  instance_class = var.mysql_instance_class

  db_name                     = var.mysql_database
  username                    = var.mysql_admin_username
  manage_master_user_password = true

  allocated_storage = var.mysql_allocated_storage
  storage_type      = "gp3"
  storage_encrypted = true

  db_subnet_group_name   = aws_db_subnet_group.mysql.name
  vpc_security_group_ids = [aws_security_group.mysql.id]
  parameter_group_name   = aws_db_parameter_group.mysql.name
  publicly_accessible    = false
  multi_az               = false

  backup_retention_period    = 1
  auto_minor_version_upgrade = true
  apply_immediately          = true

  # Ambiente de curso: destroy sin snapshot final ni protección.
  deletion_protection = false
  skip_final_snapshot = true
}

# ================================================================ DocumentDB

resource "aws_docdb_subnet_group" "docdb" {
  name       = "${var.project_name}-docdb"
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_docdb_cluster_parameter_group" "docdb" {
  name        = "${var.project_name}-docdb50"
  family      = "docdb5.0"
  description = "DocumentDB del data warehouse: TLS obligatorio"

  parameter {
    name  = "tls"
    value = "enabled"
  }
}

resource "aws_docdb_cluster" "docdb" {
  cluster_identifier = "${var.project_name}-docdb"
  engine             = "docdb"
  engine_version     = var.docdb_engine_version

  master_username             = var.docdb_admin_username
  manage_master_user_password = true

  db_subnet_group_name            = aws_docdb_subnet_group.docdb.name
  vpc_security_group_ids          = [aws_security_group.docdb.id]
  db_cluster_parameter_group_name = aws_docdb_cluster_parameter_group.docdb.name
  storage_encrypted               = true

  backup_retention_period = 1
  apply_immediately       = true
  deletion_protection     = false
  skip_final_snapshot     = true
}

resource "aws_docdb_cluster_instance" "docdb" {
  identifier         = "${var.project_name}-docdb-1"
  cluster_identifier = aws_docdb_cluster.docdb.id
  instance_class     = var.docdb_instance_class
  apply_immediately  = true
}
