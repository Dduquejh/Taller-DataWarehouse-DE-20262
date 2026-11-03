# ---------------------------------------------------------------- General

variable "aws_region" {
  description = "Región AWS de todo el ambiente"
  type        = string
  default     = "us-east-2"
}

variable "aws_profile" {
  description = "Perfil de AWS CLI con permisos de administrador para aplicar el stack"
  type        = string
  default     = null
}

variable "project_name" {
  description = "Prefijo de los nombres de recursos (minúsculas y guiones)"
  type        = string
  default     = "ecommerce-dw"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,30}$", var.project_name))
    error_message = "project_name: minúsculas, números y guiones, de 3 a 31 caracteres."
  }
}

variable "environment" {
  description = "Entorno de despliegue"
  type        = string
  default     = "dev"
}

# ---------------------------------------------------------------- Red

variable "vpc_cidr" {
  description = "CIDR de la VPC del data warehouse"
  type        = string
  default     = "10.20.0.0/16"
}

# IPs que pueden abrir el UI de Airflow por http://<IP pública>:8080. Es
# HTTP sin cifrar: solo /32 concretos. Vacía, el UI queda disponible con el
# túnel de Session Manager.
variable "allowed_cidrs" {
  description = "CIDRs que pueden entrar al UI de Airflow (8080), p. ej. [\"203.0.113.10/32\"]"
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for cidr in var.allowed_cidrs :
      can(cidrhost(cidr, 0)) && !endswith(cidr, "/0")
    ])
    error_message = "allowed_cidrs solo acepta CIDRs válidos y restringidos, nunca 0.0.0.0/0."
  }
}

# ---------------------------------------------------------------- S3

variable "s3_force_destroy" {
  description = "Permite que terraform destroy borre el bucket aunque tenga objetos"
  type        = bool
  default     = true
}

variable "noncurrent_version_days" {
  description = "Días que se conservan las versiones anteriores de un objeto"
  type        = number
  default     = 30
}

# ---------------------------------------------------------------- RDS MySQL

variable "mysql_engine_version" {
  description = "Versión de MySQL en RDS (la local es 8.4)"
  type        = string
  default     = "8.4"
}

variable "mysql_instance_class" {
  description = "Clase de la instancia RDS"
  type        = string
  default     = "db.t4g.micro"
}

variable "mysql_allocated_storage" {
  description = "GiB de almacenamiento gp3 de RDS"
  type        = number
  default     = 20
}

variable "mysql_database" {
  description = "Base de datos de la fuente relacional"
  type        = string
  default     = "ecommerce"
}

variable "mysql_admin_username" {
  description = "Usuario maestro de RDS; la contraseña la genera y guarda Secrets Manager"
  type        = string
  default     = "dw_admin"
}

# ---------------------------------------------------------------- DocumentDB

variable "docdb_engine_version" {
  description = "Versión de DocumentDB (compatible con la API de MongoDB 5.0)"
  type        = string
  default     = "5.0.0"
}

variable "docdb_instance_class" {
  description = "Clase de la instancia DocumentDB"
  type        = string
  default     = "db.t4g.medium"
}

variable "docdb_admin_username" {
  description = "Usuario maestro de DocumentDB; la contraseña la genera y guarda Secrets Manager"
  type        = string
  default     = "docdb_admin"
}

variable "mongo_database" {
  description = "Base de datos de la fuente documental"
  type        = string
  default     = "ecommerce"
}

# ---------------------------------------------------------------- Redshift

variable "redshift_database" {
  description = "Base de datos del data warehouse en Redshift Serverless"
  type        = string
  default     = "ecommerce_dw"
}

variable "redshift_admin_username" {
  description = "Usuario administrador de Redshift; la contraseña la genera y guarda Secrets Manager"
  type        = string
  default     = "dw_admin"
}

variable "redshift_base_capacity" {
  description = "RPU base del workgroup (8 es el mínimo)"
  type        = number
  default     = 8
}

variable "redshift_max_capacity" {
  description = "RPU máximas a las que puede escalar el workgroup"
  type        = number
  default     = 8
}

# Si se alcanza el límite, Redshift desactiva el workgroup hasta el día
# siguiente: evita que una consulta olvidada consuma los créditos.
variable "redshift_daily_rpu_hours_limit" {
  description = "Horas-RPU diarias máximas antes de desactivar el workgroup"
  type        = number
  default     = 16
}

# ---------------------------------------------------------------- EC2 / Airflow

variable "instance_type" {
  description = "Tipo de instancia de Airflow y dbt (mínimo 4 GiB de RAM)"
  type        = string
  default     = "t3.medium"
}

variable "root_volume_size" {
  description = "GiB del disco raíz gp3 de la instancia"
  type        = number
  default     = 30
}

variable "ami_ssm_parameter" {
  description = "Parámetro público de SSM con la AMI de Amazon Linux 2023"
  type        = string
  default     = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

variable "key_name" {
  description = "Par de llaves EC2 para SSH (opcional; sin él se usa Session Manager)"
  type        = string
  default     = null
}

variable "repo_url" {
  description = "URL HTTPS del repositorio que se clona en la instancia"
  type        = string
  default     = "https://github.com/Dduquejh/Taller-DataWarehouse-DE-20262.git"
}

variable "repo_branch" {
  description = "Rama que se despliega (debe existir en GitHub)"
  type        = string
  default     = "main"
}

variable "docker_compose_version" {
  description = "Versión del plugin docker compose"
  type        = string
  default     = "v5.6.1"
}

variable "docker_buildx_version" {
  description = "Versión del plugin docker buildx"
  type        = string
  default     = "v0.38.1"
}

# ---------------------------------------------------------------- QuickSight

# La conexión VPC solo puede crearse después de suscribir la cuenta a
# QuickSight (paso manual en la consola). Primer apply con false; después
# de suscribirse, apply con true.
variable "enable_quicksight_vpc_connection" {
  description = "Crea la conexión VPC de QuickSight hacia Redshift"
  type        = bool
  default     = false
}

# IDs de los datasets SPICE que el pipeline refresca al terminar un lote.
# Se conocen después de crear el dataset en QuickSight.
variable "quicksight_dataset_ids" {
  description = "Datasets SPICE a refrescar tras cada lote, p. ej. [\"sales-dashboard\"]"
  type        = list(string)
  default     = []
}

# ---------------------------------------------------------------- Costos

variable "budget_email" {
  description = "Correo para alertas de presupuesto; null no crea el presupuesto"
  type        = string
  default     = null
}

variable "monthly_budget_usd" {
  description = "Presupuesto mensual en USD del proyecto"
  type        = number
  default     = 30
}
