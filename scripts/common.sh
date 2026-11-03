#!/usr/bin/env bash
# Funciones compartidas por los scripts. Se ejecutan en la instancia EC2,
# desde la raíz del repo (/opt/<proyecto>), donde dw-render-env dejó .env.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker/docker-compose.aws.yml"
DAG_ID="ecommerce_pipeline"

compose() {
  docker compose --project-directory "$PROJECT_ROOT" -f "$COMPOSE_FILE" "$@"
}

require_env_file() {
  [[ -f "$PROJECT_ROOT/.env" ]] || {
    printf 'Error: falta .env. En la instancia ejecute: sudo dw-render-env\n' >&2
    exit 1
  }
}

# Ejecuta un comando dentro del scheduler, que tiene el código, el rol de la
# instancia y las variables de .env.
in_airflow() {
  require_env_file
  compose exec -T airflow-scheduler "$@"
}

pipeline_module() {
  in_airflow python -m "$@"
}
