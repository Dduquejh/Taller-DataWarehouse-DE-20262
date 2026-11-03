#!/usr/bin/env bash
# Detiene Airflow conservando su base de metadatos.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
compose stop
