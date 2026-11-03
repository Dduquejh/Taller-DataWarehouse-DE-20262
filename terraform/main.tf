data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

# Redshift Serverless exige subredes en tres zonas de disponibilidad.
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

locals {
  account_id = data.aws_caller_identity.current.account_id
  azs        = slice(data.aws_availability_zones.available.names, 0, 3)

  bucket_name = "${var.project_name}-${local.account_id}"
  bucket_arn  = "arn:${data.aws_partition.current.partition}:s3:::${local.bucket_name}"

  # Prefijos del bucket. raw guarda las extracciones por fuente, entidad y
  # lote; control guarda el registro de cada ejecución del pipeline.
  raw_prefix     = "raw/"
  control_prefix = "control/"

  # Carpeta del repo en la instancia: fuera del home para que ec2-user y
  # ssm-user la encuentren igual.
  app_dir = "/opt/${var.project_name}"

  # Parámetro de SSM con la configuración no secreta del pipeline.
  config_parameter_name = "/${var.project_name}/pipeline/env"

  profile_flag = var.aws_profile == null ? "" : " --profile ${var.aws_profile}"

  # Nombres sin guiones para motores que no los aceptan.
  name_underscore = replace(var.project_name, "-", "_")
}
