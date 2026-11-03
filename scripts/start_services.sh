#!/usr/bin/env bash
# Construye la imagen y levanta Airflow (Postgres, webserver y scheduler).
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_env_file
mkdir -p "$PROJECT_ROOT/logs"
compose up -d --build
compose ps
