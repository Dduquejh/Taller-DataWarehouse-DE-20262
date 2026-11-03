#!/usr/bin/env bash
# Crea las tablas de RDS y las colecciones de DocumentDB y carga datos
# sintéticos (reemplaza los existentes). Acepta las opciones de
# generate_data.py, p. ej. --seed 42 --bad-records 2.
#
# Después de regenerar, el siguiente lote debe ser una carga completa:
#   scripts/run_pipeline.sh --mode full
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
printf 'Generando datos en RDS MySQL y DocumentDB...\n'
pipeline_module ecommerce_dw.sources.generate_data "$@"
