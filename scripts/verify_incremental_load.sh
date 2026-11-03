#!/usr/bin/env bash
# Evidencia de carga incremental e idempotencia, de extremo a extremo:
#   1. El warehouse concilia con la fuente validada.
#   2. Un lote nuevo en las fuentes agrega 3 filas a cada hecho.
#   3. Repetir el mismo lote en las fuentes no cambia los hechos.
#   4. Reprocesar desde S3 el último lote no cambia los hechos.
#
#   scripts/verify_incremental_load.sh --batch-id N    (N no usado antes)
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
[[ "$#" -eq 2 && "$1" == "--batch-id" && "$2" =~ ^[0-9]+$ ]] || {
  printf 'Uso: %s --batch-id NUMERO_NO_USADO\n' "$0" >&2
  exit 2
}
batch_id="$2"
scripts_dir="$PROJECT_ROOT/scripts"

fact_state() {
  "$scripts_dir/warehouse.sh" state | awk '$1 ~ /^fct_/ { print $1, $2, $3, $4, $5 }'
}
fact_rows() {
  awk -v name="$2" '$1 == name { print $2 }' <<< "$1"
}

"$scripts_dir/warehouse.sh" reconcile
baseline="$(fact_state)"
printf 'Estado inicial:\n%s\n\n' "$baseline"

append_output="$("$scripts_dir/append_batch.sh" --batch-id "$batch_id")"
printf '%s\n' "$append_output"
[[ "$append_output" == *"3 new orders"* ]] || {
  printf 'Error: el lote %s ya existía en las fuentes. Use otro número.\n' "$batch_id" >&2
  exit 1
}
"$scripts_dir/run_pipeline.sh"
after_new="$(fact_state)"
"$scripts_dir/warehouse.sh" reconcile
for fact in fct_sales fct_reviews fct_navigation_events; do
  expected=$(( $(fact_rows "$baseline" "$fact") + 3 ))
  actual="$(fact_rows "$after_new" "$fact")"
  [[ "$actual" -eq "$expected" ]] || {
    printf 'Error: %s tiene %s filas; se esperaban %s\n' "$fact" "$actual" "$expected" >&2
    exit 1
  }
done
printf 'OK lote nuevo: cada hecho creció en 3 filas\n%s\n\n' "$after_new"

repeat_output="$("$scripts_dir/append_batch.sh" --batch-id "$batch_id")"
printf '%s\n' "$repeat_output"
"$scripts_dir/run_pipeline.sh"
after_repeat="$(fact_state)"
[[ "$after_repeat" == "$after_new" ]] || {
  printf 'Error: repetir el lote %s cambió los hechos\n%s\n---\n%s\n' "$batch_id" "$after_new" "$after_repeat" >&2
  exit 1
}
printf 'OK repetición del lote en las fuentes: hechos sin cambios\n\n'

last_batch="$("$scripts_dir/warehouse.sh" query \
  "select max(batch_id) as batch_id from control.batch_entities where status = 'processed'" | tail -1 | tr -d ' ')"
"$scripts_dir/run_pipeline.sh" --reprocess "$last_batch"
after_reprocess="$(fact_state)"
[[ "$after_reprocess" == "$after_new" ]] || {
  printf 'Error: reprocesar el lote %s cambió los hechos\n%s\n---\n%s\n' "$last_batch" "$after_new" "$after_reprocess" >&2
  exit 1
}
printf 'OK reprocesamiento desde S3 del lote %s: hechos sin cambios\n' "$last_batch"
