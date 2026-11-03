#!/usr/bin/env bash
# Cambia ciudad de un cliente y categoría de un producto en RDS (SCD tipo 2).
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
printf 'Aplicando cambios en la fuente...\n'
pipeline_module ecommerce_dw.sources.simulate_changes "$@"
