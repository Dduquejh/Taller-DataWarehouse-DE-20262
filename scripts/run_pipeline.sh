#!/usr/bin/env bash
# Dispara el DAG ecommerce_pipeline y espera su resultado.
#
#   scripts/run_pipeline.sh                       # lote automático (full la primera vez, luego incremental)
#   scripts/run_pipeline.sh --mode full           # copia completa (p. ej. tras generate_data.sh)
#   scripts/run_pipeline.sh --reprocess 20261103T120000Z   # recarga un lote desde S3
#   scripts/run_pipeline.sh --no-wait
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

mode="auto"
reprocess=""
wait_for_result=1
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --mode) mode="$2"; shift 2 ;;
    --reprocess) reprocess="$2"; shift 2 ;;
    --no-wait) wait_for_result=0; shift ;;
    *) printf 'Opción desconocida: %s\n' "$1" >&2; exit 2 ;;
  esac
done

run_id="manual__$(date -u +%Y%m%dT%H%M%S)"
conf="$(printf '{"mode": "%s", "reprocess_batch_id": "%s"}' "$mode" "$reprocess")"

in_airflow airflow dags unpause "$DAG_ID" >/dev/null
in_airflow airflow dags trigger "$DAG_ID" --run-id "$run_id" --conf "$conf" >/dev/null
printf 'DAG %s disparado: run_id=%s conf=%s\n' "$DAG_ID" "$run_id" "$conf"
[[ "$wait_for_result" -eq 1 ]] || exit 0

state=""
for _ in $(seq 1 240); do
  state="$(in_airflow airflow dags list-runs -d "$DAG_ID" -o json 2>/dev/null \
    | jq -r --arg id "$run_id" '.[] | select(.run_id == $id) | .state')"
  case "$state" in
    success|failed) break ;;
  esac
  sleep 15
done

printf 'Resultado de %s: %s\n' "$run_id" "${state:-desconocido}"
"$PROJECT_ROOT/scripts/warehouse.sh" runs | head -4 || true
[[ "$state" == "success" ]]
