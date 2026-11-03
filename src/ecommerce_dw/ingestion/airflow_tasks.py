"""Funciones que ejecutan las tareas del DAG ecommerce_pipeline.

Cada función recibe el contexto del lote (batch) que publica la primera
tarea por XCom y registra en sus logs el prefijo [batch=<id>], de modo que
un fallo se puede ubicar por lote y por tarea.
"""

import json
import logging
import os
import re
from contextlib import closing
from datetime import datetime, timezone

from ..connections import bucket, mongo_client, mysql_connection, redshift_connection, s3_client
from .raw_sources import MONGO_ENTITIES, MYSQL_ENTITIES
from .redshift_loader import ensure_warehouse, load_batch, mark_processed, plan_batch, record_run
from .s3_batches import extract_mongo, extract_mysql, manifests_for_batch

LOGGER = logging.getLogger(__name__)

VALID_MODES = ("auto", "full", "incremental")
BATCH_ID_PATTERN = re.compile(r"^\d{8}T\d{6}Z$")
REQUIRED_ENV = [
    "AWS_REGION", "S3_BUCKET", "REDSHIFT_HOST", "REDSHIFT_DATABASE", "REDSHIFT_COPY_ROLE_ARN",
]
SOURCE_ENV = ["MYSQL_HOST", "DOCDB_HOST"]
# Variable de Airflow para demostrar un fallo controlado: extract, load o dbt.
FAILURE_VARIABLE = "ecommerce_dw_fail_stage"


class SimulatedFailure(RuntimeError):
    """Fallo provocado a propósito para demostrar la recuperación."""


def maybe_fail(stage, batch_id):
    from airflow.models import Variable

    if Variable.get(FAILURE_VARIABLE, default_var="").strip().lower() == stage:
        raise SimulatedFailure(
            f"[batch={batch_id}] fallo simulado en la etapa '{stage}' "
            f"(borre la variable {FAILURE_VARIABLE} y limpie la tarea para reintentar)"
        )


def batch_id_for(context):
    """Identificador del lote: instante lógico de la ejecución en UTC.

    Es estable entre reintentos y al limpiar tareas, y ordenable como texto.
    """
    logical = context["logical_date"].in_timezone("UTC")
    return logical.strftime("%Y%m%dT%H%M%SZ")


# ---------------------------------------------------------------- 1. Validación

def validate_and_prepare(**context):
    """Valida parámetros y conexiones, prepara Redshift y planifica el lote."""
    params = context["params"]
    mode = (params.get("mode") or "auto").lower()
    if mode not in VALID_MODES:
        raise ValueError(f"mode debe ser uno de {VALID_MODES}, no {mode!r}")
    reprocess_id = (params.get("reprocess_batch_id") or "").strip()
    if reprocess_id and not BATCH_ID_PATTERN.match(reprocess_id):
        raise ValueError(f"reprocess_batch_id debe tener el formato AAAAMMDDTHHMMSSZ, no {reprocess_id!r}")
    batch_id = reprocess_id or batch_id_for(context)

    required = REQUIRED_ENV + ([] if reprocess_id else SOURCE_ENV)
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise EnvironmentError(f"Faltan variables de entorno: {', '.join(missing)}")

    s3_client().head_bucket(Bucket=bucket())
    LOGGER.info("[batch=%s] S3 s3://%s accesible", batch_id, bucket())

    with closing(redshift_connection()) as connection, connection.cursor() as cursor:
        cursor.execute("select current_user, current_database(), version()")
        LOGGER.info("[batch=%s] Redshift: %s", batch_id, cursor.fetchone())
    ensure_warehouse()

    if not reprocess_id:
        with closing(mysql_connection()) as connection, connection.cursor() as cursor:
            cursor.execute("select version()")
            LOGGER.info("[batch=%s] MySQL %s", batch_id, cursor.fetchone()[0])
        with closing(mongo_client()) as client:
            info = client.admin.command("ping")
            LOGGER.info("[batch=%s] DocumentDB ping: %s", batch_id, info)

    plan = {} if reprocess_id else plan_batch(batch_id, mode)
    batch = {
        "batch_id": batch_id,
        "mode": "reprocess" if reprocess_id else mode,
        "reprocess": bool(reprocess_id),
        "plan": plan,
        "dag_run_id": context["run_id"],
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    LOGGER.info("[batch=%s] plan: %s", batch_id, json.dumps(batch, indent=2))
    return batch


# ---------------------------------------------------------------- 2-3. Extracción a S3

def extract_mysql_task(batch):
    maybe_fail("extract", batch["batch_id"])
    if batch["reprocess"]:
        LOGGER.info("[batch=%s] reprocesamiento: se usan los archivos de S3, no MySQL", batch["batch_id"])
        return manifests_for_batch(MYSQL_ENTITIES, batch["batch_id"])
    return extract_mysql(batch["batch_id"], batch["plan"])


def extract_mongo_task(batch):
    if batch["reprocess"]:
        LOGGER.info("[batch=%s] reprocesamiento: se usan los archivos de S3, no DocumentDB", batch["batch_id"])
        return manifests_for_batch(MONGO_ENTITIES, batch["batch_id"])
    return extract_mongo(batch["batch_id"], batch["plan"])


# ---------------------------------------------------------------- 4. Carga a Redshift

def load_raw_task(batch, mysql_manifests, mongo_manifests):
    manifests = {**mysql_manifests, **mongo_manifests}
    results = load_batch(
        batch["batch_id"],
        manifests,
        batch["dag_run_id"],
        after_first_copy=lambda: maybe_fail("load", batch["batch_id"]),
    )
    LOGGER.info("[batch=%s] raw cargado: %s", batch["batch_id"], results)
    return results


# ---------------------------------------------------------------- 7. Registro del resultado

def _task_states(dag_run, own_task_id):
    return {
        ti.task_id: ti.state
        for ti in dag_run.get_task_instances()
        if ti.task_id != own_task_id
    }


def _refresh_spice(batch_id):
    """Refresca los datasets SPICE de QUICKSIGHT_DATASET_IDS (opcional)."""
    dataset_ids = [value.strip() for value in os.getenv("QUICKSIGHT_DATASET_IDS", "").split(",") if value.strip()]
    if not dataset_ids:
        return {}
    import boto3

    account_id = boto3.client("sts").get_caller_identity()["Account"]
    quicksight = boto3.client("quicksight")
    ingestions = {}
    for dataset_id in dataset_ids:
        ingestion_id = f"batch-{batch_id}-{datetime.now(timezone.utc):%H%M%S}"
        quicksight.create_ingestion(AwsAccountId=account_id, DataSetId=dataset_id, IngestionId=ingestion_id)
        ingestions[dataset_id] = ingestion_id
        LOGGER.info("[batch=%s] SPICE: ingesta %s iniciada para %s", batch_id, ingestion_id, dataset_id)
    return ingestions


def record_result(**context):
    """Registra el resultado del lote en Redshift y en S3 (se ejecuta siempre).

    Si alguna tarea anterior falló, registra el fallo y falla también, para
    que la ejecución del DAG quede marcada como fallida.
    """
    ti = context["ti"]
    batch = ti.xcom_pull(task_ids="validate_and_prepare") or {}
    batch_id = batch.get("batch_id") or batch_id_for(context)
    states = _task_states(context["dag_run"], ti.task_id)
    failed = sorted(task for task, state in states.items() if state != "success")
    status = "success" if not failed else "failed"

    raw_rows = ti.xcom_pull(task_ids="load_raw") or {}
    dbt_summary = ti.xcom_pull(task_ids="dbt_test") or {}
    details = {"states": states, "raw_rows": raw_rows, "dbt_test": dbt_summary, "plan": batch.get("plan")}

    if status == "success":
        details["entities_processed"] = mark_processed(batch_id)
        try:
            details["spice"] = _refresh_spice(batch_id)
        except Exception as error:  # el dashboard no debe invalidar un lote ya procesado
            LOGGER.warning("[batch=%s] no se pudo refrescar SPICE: %s", batch_id, error)
            details["spice_error"] = str(error)

    record_run(
        batch_id, context["run_id"], batch.get("mode"), batch.get("reprocess"), status,
        batch.get("started_at"), failed, sum(raw_rows.values()) if raw_rows else None, dbt_summary, details,
    )
    key = f"{os.getenv('S3_CONTROL_PREFIX', 'control').strip('/')}/runs/batch_id={batch_id}/{context['run_id']}.json"
    s3_client().put_object(
        Bucket=bucket(), Key=key, ContentType="application/json",
        Body=json.dumps({"batch_id": batch_id, "status": status, "failed_tasks": failed, **details},
                        indent=2, default=str).encode(),
    )
    LOGGER.info("[batch=%s] resultado %s registrado en control.pipeline_runs y s3://%s/%s",
                batch_id, status, bucket(), key)
    if failed:
        raise RuntimeError(f"[batch={batch_id}] tareas fallidas: {', '.join(failed)}")
    return {"batch_id": batch_id, "status": status}
