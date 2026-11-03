#!/usr/bin/env bash
# Consultas de evidencia sobre Redshift:
#   scripts/warehouse.sh state | reconcile | batches | runs | quarantine
#   scripts/warehouse.sh scd2 --customer-id 1 --product-id 1
#   scripts/warehouse.sh query "select count(*) from marts.fct_sales"
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
pipeline_module ecommerce_dw.evidence "$@"
