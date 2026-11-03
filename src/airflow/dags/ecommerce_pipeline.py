"""Pipeline de extremo a extremo en AWS: RDS y DocumentDB -> S3 -> Redshift -> dbt.

Etapas (dependencias explícitas, sin intervención manual entre ellas):

    validate_and_prepare          1. parámetros, conexiones, DDL de control y plan del lote
      -> extract.mysql / extract.documentdb   2-3. extracción a Parquet en S3 (en paralelo)
      -> load_raw                 4. COPY a raw en una sola transacción por lote
      -> dbt_deps -> dbt_seed -> dbt_run_intermediate -> dbt_run_quarantine
      -> dbt_snapshot -> dbt_run_marts                  5. transformaciones y snapshots
      -> dbt_test                 6. pruebas de calidad
      -> record_result            7. registro del resultado (siempre se ejecuta)

Parámetros al disparar el DAG:
- mode: auto (por defecto), full o incremental.
- reprocess_batch_id: recarga un lote existente desde S3 sin consultar las
  fuentes y vuelve a ejecutar dbt.

Fallo controlado: la variable de Airflow ecommerce_dw_fail_stage (extract,
load o dbt) hace fallar esa etapa. Para recuperar: borrar la variable y
limpiar (Clear) la tarea fallida; el lote conserva su batch_id.
"""

from datetime import timedelta

import pendulum
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from airflow import DAG
from ecommerce_dw.ingestion.airflow_tasks import (
    extract_mongo_task,
    extract_mysql_task,
    load_raw_task,
    record_result,
    validate_and_prepare,
)
from ecommerce_dw.transformations.dbt_runner import run_dbt

BATCH = "{{ ti.xcom_pull(task_ids='validate_and_prepare') }}"

with DAG(
    dag_id="ecommerce_pipeline",
    description="RDS + DocumentDB -> S3 (Parquet) -> Redshift (COPY) -> dbt -> QuickSight",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Bogota"),
    # Un lote diario a las 06:00 de Bogotá; también se puede disparar a mano.
    schedule="0 6 * * *",
    catchup=False,
    # Un solo lote a la vez: evita marcas de agua y cargas concurrentes.
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    params={
        "mode": Param("auto", type="string", enum=["auto", "full", "incremental"],
                      description="Tipo de extracción del lote"),
        "reprocess_batch_id": Param("", type="string",
                                    description="Lote existente (AAAAMMDDTHHMMSSZ) a reprocesar desde S3"),
    },
    tags=["aws", "redshift", "dbt"],
) as dag:
    validate = PythonOperator(
        task_id="validate_and_prepare",
        python_callable=validate_and_prepare,
        retries=1,
    )

    with TaskGroup(group_id="extract", tooltip="Extracción de las fuentes a S3"):
        extract_mysql = PythonOperator(
            task_id="mysql", python_callable=extract_mysql_task, op_args=[BATCH],
        )
        extract_documentdb = PythonOperator(
            task_id="documentdb", python_callable=extract_mongo_task, op_args=[BATCH],
        )

    load = PythonOperator(
        task_id="load_raw",
        python_callable=load_raw_task,
        op_args=[BATCH, extract_mysql.output, extract_documentdb.output],
    )

    def dbt_task(task_id, command, selector=None, **kwargs):
        return PythonOperator(
            task_id=task_id,
            python_callable=run_dbt,
            op_kwargs={"command": command, "selector": selector, "batch": BATCH},
            **kwargs,
        )

    deps = dbt_task("dbt_deps", "deps")
    seed = dbt_task("dbt_seed", "seed")
    intermediate = dbt_task("dbt_run_intermediate", "run", "path:models/intermediate")
    quarantine = dbt_task("dbt_run_quarantine", "run", "path:models/quarantine")
    snapshots = dbt_task("dbt_snapshot", "snapshot")
    marts = dbt_task("dbt_run_marts", "run", "path:models/marts")
    # Las pruebas que fallan no se reintentan: el resultado no cambiaría.
    tests = dbt_task("dbt_test", "test", retries=0)

    record = PythonOperator(
        task_id="record_result",
        python_callable=record_result,
        trigger_rule=TriggerRule.ALL_DONE,
        # Falla a propósito cuando el lote falló; reintentarla no cambia nada.
        retries=0,
    )

    validate >> [extract_mysql, extract_documentdb] >> load
    load >> deps >> seed >> intermediate >> quarantine >> snapshots >> marts >> tests >> record
