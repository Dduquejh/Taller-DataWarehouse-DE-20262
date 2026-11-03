#!/usr/bin/env bash
# Agrega a las fuentes un lote diario reproducible (pedidos, reseñas y eventos).
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
[[ "$#" -eq 2 && "$1" == "--batch-id" ]] || {
  printf 'Uso: %s --batch-id NUMERO\n' "$0" >&2
  exit 2
}
printf 'Agregando a las fuentes el lote diario %s...\n' "$2"
pipeline_module ecommerce_dw.sources.append_batch "$@"
