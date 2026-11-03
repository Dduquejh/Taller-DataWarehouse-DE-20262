#!/usr/bin/env bash
# Ejecuta un comando de dbt contra Redshift fuera del DAG, p. ej.:
#   scripts/run_dbt.sh test
#   scripts/run_dbt.sh run path:models/marts
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
[[ "$#" -ge 1 ]] || { printf 'Uso: %s COMANDO [SELECTOR]\n' "$0" >&2; exit 2; }
pipeline_module ecommerce_dw.transformations.dbt_runner "$@"
