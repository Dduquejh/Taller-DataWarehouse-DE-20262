# Conexión VPC de QuickSight: QuickSight crea interfaces de red en las
# subredes privadas y llega a Redshift sin que este sea público.
#
# Requisitos (manuales, una sola vez por cuenta):
#   1. Suscribir la cuenta a QuickSight en la consola, en var.aws_region.
#   2. terraform apply -var enable_quicksight_vpc_connection=true
#   3. En QuickSight: Datasets > New > Redshift (manual connect), con esta
#      conexión VPC, el endpoint de Redshift y el usuario de solo lectura.

resource "aws_quicksight_vpc_connection" "redshift" {
  count = var.enable_quicksight_vpc_connection ? 1 : 0

  vpc_connection_id  = "${var.project_name}-redshift"
  name               = "${var.project_name}-redshift"
  role_arn           = aws_iam_role.quicksight_vpc[0].arn
  security_group_ids = [aws_security_group.quicksight.id]
  subnet_ids         = aws_subnet.private[*].id

  depends_on = [aws_iam_role_policy.quicksight_vpc]
}
