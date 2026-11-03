# Alerta de costos del proyecto: avisa al 50 %, 80 % y 100 % del gasto
# real y cuando el pronóstico supera el presupuesto. Filtra por la etiqueta
# Project, que el provider agrega a todos los recursos (default_tags).
#
# La etiqueta debe estar activada como "cost allocation tag" en Billing
# para que el filtro tenga efecto (paso manual, tarda hasta 24 h).

resource "aws_budgets_budget" "project" {
  count = var.budget_email == null ? 0 : 1

  name         = "${var.project_name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "TagKeyValue"
    values = [format("user:Project$%s", var.project_name)]
  }

  dynamic "notification" {
    for_each = [50, 80, 100]

    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.budget_email]
    }
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_email]
  }
}
