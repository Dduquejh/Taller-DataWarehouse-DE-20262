"""Ejecuta dbt contra Redshift desde las tareas de Airflow.

dbt corre en la instancia EC2 (venv propio dentro de la imagen), pero el SQL
se ejecuta en Redshift. Las credenciales se leen de Secrets Manager y se
pasan a dbt solo como variables de entorno del subproceso.
"""

import json
import logging
import os
import subprocess
from pathlib import Path

from ..connections import redshift_credentials

DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt")
DBT_PROJECT = Path(os.getenv("DBT_PROJECT_DIR", "/opt/airflow/src/dbt"))
# El código se monta desde el repo (propiedad de ec2-user); los artefactos de
# dbt van a una carpeta escribible del contenedor.
DBT_STATE_DIR = Path(os.getenv("DBT_STATE_DIR", "/tmp/dbt"))
TARGET_PATH = DBT_STATE_DIR / "target"
LOGGER = logging.getLogger(__name__)


def _environment():
    user, password = redshift_credentials()
    environment = dict(os.environ)
    environment.update({
        "DBT_REDSHIFT_USER": user,
        "DBT_REDSHIFT_PASSWORD": password,
        "DBT_TARGET_PATH": str(TARGET_PATH),
        "DBT_LOG_PATH": str(DBT_STATE_DIR / "logs"),
        "DBT_PACKAGES_DIR": str(DBT_STATE_DIR / "dbt_packages"),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    })
    return environment


def _summary(command):
    """Cuenta los resultados de run_results.json (pass, fail, error, ...)."""
    path = TARGET_PATH / "run_results.json"
    if command == "deps" or not path.exists():
        return {}
    summary = {}
    for result in json.loads(path.read_text())["results"]:
        status = result["status"]
        summary[status] = summary.get(status, 0) + 1
        if status in ("fail", "error"):
            LOGGER.error("dbt %s: %s -> %s (%s)", command, result["unique_id"], status, result.get("message"))
    return summary


def run_dbt(command, selector=None, batch=None):
    """Ejecuta un comando de dbt y devuelve el resumen de resultados."""
    batch_id = (batch or {}).get("batch_id", "-")
    if command == "run" and selector == "path:models/marts" and batch:
        from ..ingestion.airflow_tasks import maybe_fail

        maybe_fail("dbt", batch_id)

    arguments = [
        DBT_EXECUTABLE, command,
        "--project-dir", str(DBT_PROJECT),
        "--profiles-dir", str(DBT_PROJECT),
    ]
    if selector:
        arguments.extend(["--select", selector])

    LOGGER.info("[batch=%s] dbt: %s", batch_id, " ".join(arguments))
    # Sin el resultado anterior, un fallo temprano no reporta conteos viejos.
    (TARGET_PATH / "run_results.json").unlink(missing_ok=True)
    completed = subprocess.run(arguments, env=_environment(), check=False)
    summary = _summary(command)
    LOGGER.info("[batch=%s] dbt %s: %s", batch_id, command, summary)
    if completed.returncode != 0:
        raise RuntimeError(f"[batch={batch_id}] dbt {command} terminó con código {completed.returncode}: {summary}")
    return summary


if __name__ == "__main__":
    # python -m ecommerce_dw.transformations.dbt_runner <comando> [selector]
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if len(sys.argv) < 2:
        sys.exit("uso: python -m ecommerce_dw.transformations.dbt_runner <comando> [selector]")
    run_dbt(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)
