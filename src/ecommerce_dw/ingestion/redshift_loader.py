"""Carga de lotes a Redshift con COPY y tablas de control.

Esquemas:
- raw: una tabla por entidad, de solo anexado. Cada fila conserva _batch_id;
  recargar un lote borra primero sus filas, así repetirlo no duplica.
- control.batch_entities: una fila por lote y entidad cargados (marca de agua,
  conteos, estado). Es la fuente de verdad del avance incremental.
- control.pipeline_runs: resultado de cada ejecución del DAG.

Estados de un lote (control.batch_entities.status):
- loaded: COPY confirmado. La marca de agua avanza desde aquí, aunque dbt
  falle después: los datos ya están seguros en raw y dbt se reejecuta sobre
  ellos sin volver a extraer.
- processed: dbt (modelos, snapshots y pruebas) terminó bien para el lote.
"""

import json
import logging
import os
from contextlib import closing

from ..connections import redshift_connection
from .raw_sources import ENTITIES, ENTITIES_BY_KEY, RAW_SCHEMA, parse_watermark

LOGGER = logging.getLogger(__name__)
LOADED_STATES = ("loaded", "processed")

CONTROL_DDL = [
    f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}",
    "CREATE SCHEMA IF NOT EXISTS control",
    """CREATE TABLE IF NOT EXISTS control.batch_entities (
        batch_id VARCHAR(32) NOT NULL,
        source VARCHAR(32) NOT NULL,
        entity VARCHAR(64) NOT NULL,
        raw_table VARCHAR(128) NOT NULL,
        mode VARCHAR(16) NOT NULL,
        watermark_column VARCHAR(64),
        watermark_from VARCHAR(64),
        watermark_to VARCHAR(64),
        row_count BIGINT NOT NULL,
        s3_uri VARCHAR(1024) NOT NULL,
        sha256 VARCHAR(64),
        extracted_at TIMESTAMP NOT NULL,
        loaded_at TIMESTAMP NOT NULL,
        processed_at TIMESTAMP,
        status VARCHAR(16) NOT NULL,
        dag_run_id VARCHAR(256)
    )""",
    """CREATE TABLE IF NOT EXISTS control.pipeline_runs (
        batch_id VARCHAR(32) NOT NULL,
        dag_run_id VARCHAR(256) NOT NULL,
        mode VARCHAR(16),
        reprocess BOOLEAN,
        status VARCHAR(16) NOT NULL,
        started_at TIMESTAMP,
        finished_at TIMESTAMP NOT NULL,
        failed_tasks VARCHAR(4096),
        raw_rows BIGINT,
        dbt_tests_passed INTEGER,
        dbt_tests_failed INTEGER,
        details VARCHAR(65535)
    )""",
]


def ensure_warehouse():
    """Crea esquemas, tablas de control y tablas raw si no existen."""
    with closing(redshift_connection()) as connection:
        with connection.cursor() as cursor:
            for statement in CONTROL_DDL:
                cursor.execute(statement)
            for entity in ENTITIES:
                cursor.execute(entity.redshift_ddl())
        connection.commit()


def plan_batch(batch_id, requested_mode):
    """Decide por entidad el modo de extracción y la marca de agua.

    - full: copia completa.
    - incremental: filas con avance > máximo watermark_to de los lotes cargados
      desde la última copia completa (excluido el lote actual).
    - auto: full si la entidad nunca tuvo una copia completa; si no, incremental.
    """
    with closing(redshift_connection()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            select source || '.' || entity, batch_id, mode, watermark_to
            from control.batch_entities
            where status in %s and batch_id < %s
            order by batch_id
            """,
            (LOADED_STATES, batch_id),
        )
        history = cursor.fetchall()

    plan = {}
    for entity in ENTITIES:
        rows = [row for row in history if row[0] == entity.key]
        full_batches = [row[1] for row in rows if row[2] == "full"]
        mode = requested_mode
        if mode == "auto":
            mode = "incremental" if full_batches else "full"
        if mode == "incremental" and not full_batches:
            raise ValueError(f"{entity.key}: no hay una carga completa previa; use mode=full o auto")
        watermark = None
        if mode == "incremental":
            last_full = max(full_batches)
            values = [parse_watermark(entity, row[3]) for row in rows if row[1] >= last_full and row[3]]
            watermark = max(values) if values else None
            watermark = None if watermark is None else (
                str(watermark) if entity.watermark_kind == "integer" else watermark.isoformat(sep=" ")
            )
        plan[entity.key] = {"mode": mode, "watermark": watermark}
    return plan


def load_batch(batch_id, manifests, dag_run_id, copy_role_arn=None, after_first_copy=None):
    """Carga todas las entidades del lote en una sola transacción.

    Si algo falla (COPY, conteo o un fallo simulado) se revierte el lote
    completo: raw y control quedan como antes y la tarea puede reintentarse.
    """
    copy_role_arn = copy_role_arn or os.environ["REDSHIFT_COPY_ROLE_ARN"]
    results = {}
    with closing(redshift_connection()) as connection:
        try:
            with connection.cursor() as cursor:
                for index, (key, manifest) in enumerate(sorted(manifests.items())):
                    entity = ENTITIES_BY_KEY[key]
                    table = entity.qualified_table
                    cursor.execute(f"delete from {table} where _batch_id = %s", (batch_id,))
                    replaced = cursor.rowcount
                    if manifest["row_count"] > 0:
                        cursor.execute(
                            f"copy {table} from %s iam_role %s format as parquet",
                            (manifest["s3_uri"], copy_role_arn),
                        )
                    cursor.execute(f"select count(*) from {table} where _batch_id = %s", (batch_id,))
                    loaded = cursor.fetchone()[0]
                    if loaded != manifest["row_count"]:
                        raise ValueError(
                            f"[batch={batch_id}] {table}: el manifiesto indica {manifest['row_count']} "
                            f"filas y se cargaron {loaded}"
                        )
                    cursor.execute(
                        "delete from control.batch_entities where batch_id = %s and raw_table = %s",
                        (batch_id, entity.raw_table),
                    )
                    cursor.execute(
                        """
                        insert into control.batch_entities (
                            batch_id, source, entity, raw_table, mode, watermark_column,
                            watermark_from, watermark_to, row_count, s3_uri, sha256,
                            extracted_at, loaded_at, status, dag_run_id
                        ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                  cast(%s as timestamptz), getdate(), 'loaded', %s)
                        """,
                        (
                            batch_id, entity.source, entity.name, entity.raw_table, manifest["mode"],
                            manifest["watermark_column"], manifest["watermark_from"],
                            manifest["watermark_to"], manifest["row_count"], manifest["s3_uri"],
                            manifest.get("sha256"), manifest["extracted_at"], dag_run_id,
                        ),
                    )
                    LOGGER.info(
                        "[batch=%s] %s: %s filas cargadas (%s reemplazadas)",
                        batch_id, table, loaded, replaced,
                    )
                    results[entity.raw_table] = loaded
                    if index == 0 and after_first_copy:
                        after_first_copy()
            connection.commit()
        except Exception:
            connection.rollback()
            LOGGER.exception("[batch=%s] carga revertida; raw y control no cambiaron", batch_id)
            raise
    return results


def mark_processed(batch_id):
    with closing(redshift_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                update control.batch_entities
                set status = 'processed', processed_at = getdate()
                where batch_id = %s and status in %s
                """,
                (batch_id, LOADED_STATES),
            )
            updated = cursor.rowcount
        connection.commit()
    return updated


def record_run(batch_id, dag_run_id, mode, reprocess, status, started_at, failed_tasks, raw_rows,
               dbt_summary, details):
    """Registra el resultado de la ejecución (reemplaza el registro previo del mismo run)."""
    dbt_summary = dbt_summary or {}
    with closing(redshift_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute("delete from control.pipeline_runs where dag_run_id = %s", (dag_run_id,))
            cursor.execute(
                """
                insert into control.pipeline_runs (
                    batch_id, dag_run_id, mode, reprocess, status, started_at, finished_at,
                    failed_tasks, raw_rows, dbt_tests_passed, dbt_tests_failed, details
                ) values (%s, %s, %s, %s, %s, cast(%s as timestamptz), getdate(), %s, %s, %s, %s, %s)
                """,
                (
                    batch_id, dag_run_id, mode, reprocess, status, started_at,
                    ",".join(failed_tasks) or None, raw_rows,
                    dbt_summary.get("pass"), dbt_summary.get("fail"),
                    json.dumps(details, default=str)[:65535],
                ),
            )
        connection.commit()
