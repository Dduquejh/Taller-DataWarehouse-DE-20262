# Red del data warehouse.
#
# - Una subred pública para la instancia de Airflow: necesita salida a
#   internet (GitHub, Docker Hub, PyPI) y una IP pública evita pagar un NAT
#   Gateway (~USD 32/mes), que costaría más que todo lo demás.
# - Tres subredes privadas, una por zona, para RDS, DocumentDB, Redshift
#   Serverless (exige tres zonas) y la conexión VPC de QuickSight. No tienen
#   ruta a internet: las fuentes y el warehouse solo son alcanzables desde
#   dentro de la VPC.
# - Un VPC endpoint gateway de S3 (gratuito) para que el COPY de Redshift
#   (enhanced VPC routing) y las escrituras de Airflow no salgan a internet.

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true # necesario para resolver los endpoints de RDS, DocumentDB y Redshift

  tags = { Name = "${var.project_name}-vpc" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = { Name = "${var.project_name}-igw" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, 0)
  availability_zone       = local.azs[0]
  map_public_ip_on_launch = true

  tags = { Name = "${var.project_name}-public-${local.azs[0]}", Tier = "public" }
}

resource "aws_subnet" "private" {
  count = length(local.azs)

  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, 10 + count.index)
  availability_zone = local.azs[count.index]

  tags = { Name = "${var.project_name}-private-${local.azs[count.index]}", Tier = "private" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = "${var.project_name}-public" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# Sin ruta 0.0.0.0/0: las subredes privadas no salen a internet.
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  tags = { Name = "${var.project_name}-private" }
}

resource "aws_route_table_association" "private" {
  count = length(aws_subnet.private)

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id, aws_route_table.private.id]

  tags = { Name = "${var.project_name}-s3" }
}

# ================================================================ Security groups
#
# Matriz de conexiones permitidas:
#
#   Airflow (EC2) ──3306──▶ RDS MySQL
#   Airflow (EC2) ──27017─▶ DocumentDB
#   Airflow (EC2) ──5439──▶ Redshift
#   QuickSight    ──5439──▶ Redshift
#   Redshift      ──443───▶ S3 (endpoint gateway, para COPY)
#
# Los grupos de las bases de datos no tienen reglas de salida (Terraform
# borra la regla por defecto): los security groups son stateful y las
# respuestas salen igual.

resource "aws_security_group" "airflow" {
  name        = "${var.project_name}-airflow"
  description = "Airflow y dbt en EC2"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project_name}-airflow" }
}

resource "aws_security_group" "mysql" {
  name        = "${var.project_name}-mysql"
  description = "RDS MySQL: solo desde Airflow"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project_name}-mysql" }
}

resource "aws_security_group" "docdb" {
  name        = "${var.project_name}-docdb"
  description = "DocumentDB: solo desde Airflow"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project_name}-docdb" }
}

resource "aws_security_group" "redshift" {
  name        = "${var.project_name}-redshift"
  description = "Redshift Serverless: desde Airflow y QuickSight"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project_name}-redshift" }
}

resource "aws_security_group" "quicksight" {
  name        = "${var.project_name}-quicksight"
  description = "Interfaces de red de la conexion VPC de QuickSight"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project_name}-quicksight" }
}

# ---------------------------------------------------------------- Airflow

# Salida libre: GitHub, Docker Hub, PyPI y las APIs de AWS cambian de IP.
resource "aws_vpc_security_group_egress_rule" "airflow_all" {
  security_group_id = aws_security_group.airflow.id
  description       = "Salida a internet y a las fuentes"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

# SSH solo desde el servicio EC2 Instance Connect (botón Connect de la consola).
data "aws_ec2_managed_prefix_list" "instance_connect" {
  name = "com.amazonaws.${var.aws_region}.ec2-instance-connect"
}

resource "aws_vpc_security_group_ingress_rule" "airflow_instance_connect" {
  security_group_id = aws_security_group.airflow.id
  description       = "SSH desde EC2 Instance Connect"
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
  prefix_list_id    = data.aws_ec2_managed_prefix_list.instance_connect.id
}

resource "aws_vpc_security_group_ingress_rule" "airflow_ui" {
  for_each = toset(var.allowed_cidrs)

  security_group_id = aws_security_group.airflow.id
  description       = "UI de Airflow desde una IP autorizada"
  ip_protocol       = "tcp"
  from_port         = 8080
  to_port           = 8080
  cidr_ipv4         = each.value
}

# ---------------------------------------------------------------- Fuentes

resource "aws_vpc_security_group_ingress_rule" "mysql_from_airflow" {
  security_group_id            = aws_security_group.mysql.id
  description                  = "MySQL desde Airflow"
  ip_protocol                  = "tcp"
  from_port                    = 3306
  to_port                      = 3306
  referenced_security_group_id = aws_security_group.airflow.id
}

resource "aws_vpc_security_group_ingress_rule" "docdb_from_airflow" {
  security_group_id            = aws_security_group.docdb.id
  description                  = "DocumentDB desde Airflow"
  ip_protocol                  = "tcp"
  from_port                    = 27017
  to_port                      = 27017
  referenced_security_group_id = aws_security_group.airflow.id
}

# ---------------------------------------------------------------- Redshift

resource "aws_vpc_security_group_ingress_rule" "redshift_from_airflow" {
  security_group_id            = aws_security_group.redshift.id
  description                  = "Redshift desde Airflow y dbt"
  ip_protocol                  = "tcp"
  from_port                    = 5439
  to_port                      = 5439
  referenced_security_group_id = aws_security_group.airflow.id
}

resource "aws_vpc_security_group_ingress_rule" "redshift_from_quicksight" {
  security_group_id            = aws_security_group.redshift.id
  description                  = "Redshift desde QuickSight"
  ip_protocol                  = "tcp"
  from_port                    = 5439
  to_port                      = 5439
  referenced_security_group_id = aws_security_group.quicksight.id
}

# COPY con enhanced VPC routing: el tráfico a S3 pasa por el endpoint gateway.
data "aws_ec2_managed_prefix_list" "s3" {
  name = "com.amazonaws.${var.aws_region}.s3"
}

resource "aws_vpc_security_group_egress_rule" "redshift_to_s3" {
  security_group_id = aws_security_group.redshift.id
  description       = "COPY desde S3 por el endpoint gateway"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  prefix_list_id    = data.aws_ec2_managed_prefix_list.s3.id
}

# ---------------------------------------------------------------- QuickSight

resource "aws_vpc_security_group_egress_rule" "quicksight_to_redshift" {
  security_group_id            = aws_security_group.quicksight.id
  description                  = "QuickSight hacia Redshift"
  ip_protocol                  = "tcp"
  from_port                    = 5439
  to_port                      = 5439
  referenced_security_group_id = aws_security_group.redshift.id
}

# Las interfaces de QuickSight no son stateful: las respuestas de Redshift
# necesitan una regla de entrada explícita.
resource "aws_vpc_security_group_ingress_rule" "quicksight_from_redshift" {
  security_group_id            = aws_security_group.quicksight.id
  description                  = "Respuestas de Redshift hacia QuickSight"
  ip_protocol                  = "tcp"
  from_port                    = 0
  to_port                      = 65535
  referenced_security_group_id = aws_security_group.redshift.id
}
