"""Extracción de las fuentes a archivos Parquet por lote en S3.

Organización:

    s3://<bucket>/raw/<fuente>/<entidad>/batch_id=<id>/data.parquet
    s3://<bucket>/raw/<fuente>/<entidad>/batch_id=<id>/_manifest.json

El manifiesto registra lote, fecha de extracción, modo, marcas de agua y
cantidad de filas. Los mismos datos van en los metadatos del Parquet, así
que un manifiesto perdido se reconstruye desde el archivo.

Ningún objeto se sobrescribe: las escrituras usan If-None-Match: * y fallan
si la llave ya existe. Si un reintento encuentra el lote ya escrito, lo
reutiliza en vez de extraer otra vez; así un lote es inmutable y se puede
reprocesar sin consultar las fuentes.
"""

import hashlib
import io
import json
import logging
import os
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from ..connections import bucket, mongo_client, mongo_database, mysql_connection, s3_client
from .raw_sources import (
    BATCH_SIZE,
    MONGO_ENTITIES,
    MYSQL_ENTITIES,
    flatten_mongo,
    format_watermark,
    normalize_value,
)

LOGGER = logging.getLogger(__name__)
METADATA_KEY = b"ecommerce_dw.batch"


class BatchAlreadyExists(RuntimeError):
    """El objeto ya existe en S3 y no se sobrescribe."""


def raw_prefix():
    return os.getenv("S3_RAW_PREFIX", "raw").strip("/")


def batch_prefix(entity, batch_id):
    return f"{raw_prefix()}/{entity.source}/{entity.name}/batch_id={batch_id}/"


def data_key(entity, batch_id):
    return batch_prefix(entity, batch_id) + "data.parquet"


def manifest_key(entity, batch_id):
    return batch_prefix(entity, batch_id) + "_manifest.json"


# ---------------------------------------------------------------- S3 helpers

def _put_new(key, body, content_type):
    """Escribe solo si la llave no existe (escritura condicional de S3)."""
    try:
        s3_client().put_object(
            Bucket=bucket(), Key=key, Body=body, ContentType=content_type, IfNoneMatch="*",
        )
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code in ("PreconditionFailed", "ConditionalRequestConflict", "412"):
            raise BatchAlreadyExists(f"s3://{bucket()}/{key} ya existe") from error
        raise


def _get(key):
    try:
        return s3_client().get_object(Bucket=bucket(), Key=key)["Body"].read()
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise


def read_manifest(entity, batch_id):
    body = _get(manifest_key(entity, batch_id))
    return json.loads(body) if body else None


def _manifest_from_parquet(entity, batch_id):
    """Reconstruye el manifiesto con los metadatos del Parquet ya escrito."""
    body = _get(data_key(entity, batch_id))
    if body is None:
        return None
    metadata = pq.read_schema(io.BytesIO(body)).metadata or {}
    manifest = json.loads(metadata[METADATA_KEY])
    manifest["sha256"] = hashlib.sha256(body).hexdigest()
    return manifest


def _write_manifest(entity, batch_id, manifest):
    try:
        _put_new(manifest_key(entity, batch_id), json.dumps(manifest, indent=2).encode(), "application/json")
    except BatchAlreadyExists:
        return read_manifest(entity, batch_id)
    return manifest


def existing_batch(entity, batch_id):
    """Manifiesto de un lote ya escrito (o reconstruido), o None si no existe."""
    manifest = read_manifest(entity, batch_id)
    if manifest:
        return manifest
    manifest = _manifest_from_parquet(entity, batch_id)
    if manifest:
        LOGGER.warning("[batch=%s] %s: manifiesto reconstruido desde el Parquet", batch_id, entity.key)
        return _write_manifest(entity, batch_id, manifest)
    return None


# ---------------------------------------------------------------- Escritura

def _build_table(entity, rows, batch_id, extracted_at):
    schema = entity.arrow_schema()
    data_columns = entity.columns
    columns = [[] for _ in schema]
    for row in rows:
        for index, (_, kind, _) in enumerate(data_columns):
            columns[index].append(normalize_value(kind, row[index]))
    count = len(columns[0]) if columns else 0
    offset = len(data_columns)
    columns[offset] = [batch_id] * count
    columns[offset + 1] = [entity.source_label] * count
    columns[offset + 2] = [extracted_at.replace(tzinfo=None)] * count
    arrays = [pa.array(values, type=field.type) for values, field in zip(columns, schema)]
    return pa.Table.from_arrays(arrays, schema=schema)


def _watermark_to(entity, table, watermark_from):
    if table.num_rows == 0:
        return watermark_from
    import pyarrow.compute as pc

    maximum = pc.max(table.column(entity.watermark_column)).as_py()
    return format_watermark(entity, maximum) if maximum is not None else watermark_from


def write_batch(entity, batch_id, mode, watermark_from, rows, extracted_at):
    """Escribe el Parquet y el manifiesto del lote. Devuelve el manifiesto."""
    table = _build_table(entity, rows, batch_id, extracted_at)
    manifest = {
        "batch_id": batch_id,
        "source": entity.source,
        "entity": entity.name,
        "raw_table": entity.raw_table,
        "mode": mode,
        "watermark_column": entity.watermark_column,
        "watermark_from": watermark_from,
        "watermark_to": _watermark_to(entity, table, watermark_from),
        "row_count": table.num_rows,
        "extracted_at": extracted_at.isoformat(),
        "format": "parquet",
        "s3_uri": f"s3://{bucket()}/{data_key(entity, batch_id)}",
    }
    table = table.replace_schema_metadata({METADATA_KEY: json.dumps(manifest).encode()})
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    body = buffer.getvalue()
    manifest["sha256"] = hashlib.sha256(body).hexdigest()

    try:
        _put_new(data_key(entity, batch_id), body, "application/vnd.apache.parquet")
    except BatchAlreadyExists:
        # Otro intento ya escribió este lote: se conserva el existente.
        LOGGER.warning("[batch=%s] %s: el Parquet ya existía; se reutiliza", batch_id, entity.key)
        return existing_batch(entity, batch_id)

    manifest = _write_manifest(entity, batch_id, manifest)
    LOGGER.info(
        "[batch=%s] %s: %s filas (%s, %s > %s) -> %s",
        batch_id, entity.key, manifest["row_count"], mode, entity.watermark_column,
        watermark_from, manifest["s3_uri"],
    )
    return manifest


# ---------------------------------------------------------------- Extracción

def _pending(entities, batch_id):
    """Separa las entidades ya escritas (reintento) de las pendientes."""
    done, pending = {}, []
    for entity in entities:
        manifest = existing_batch(entity, batch_id)
        if manifest:
            LOGGER.info("[batch=%s] %s: lote ya presente en S3, no se vuelve a extraer", batch_id, entity.key)
            done[entity.key] = manifest
        else:
            pending.append(entity)
    return done, pending


def extract_mysql(batch_id, plan):
    """Extrae las tablas de MySQL en una sola instantánea consistente.

    plan: {entity_key: {"mode": "full"|"incremental", "watermark": str|None}}
    """
    manifests, pending = _pending(MYSQL_ENTITIES, batch_id)
    if not pending:
        return manifests

    connection = mysql_connection()
    try:
        with connection.cursor() as cursor:
            # REPEATABLE READ + snapshot: pedidos y líneas salen del mismo instante.
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            extracted_at = datetime.now(timezone.utc)
            for entity in pending:
                step = plan[entity.key]
                names = ", ".join(name for name, _, _ in entity.columns)
                sql = f"SELECT {names} FROM {entity.origin}"
                params = ()
                if step["mode"] == "incremental" and step["watermark"] is not None:
                    sql += f" WHERE {entity.watermark_column} > %s"
                    params = (step["watermark"],)
                sql += f" ORDER BY {entity.watermark_column}, {entity.columns[0][0]}"
                cursor.execute(sql, params)
                rows = []
                while batch := cursor.fetchmany(BATCH_SIZE):
                    rows.extend(batch)
                manifests[entity.key] = write_batch(
                    entity, batch_id, step["mode"], step["watermark"], rows, extracted_at,
                )
        connection.rollback()  # solo lectura: cierra la instantánea
    finally:
        connection.close()
    return manifests


def extract_mongo(batch_id, plan):
    """Extrae las colecciones de DocumentDB."""
    manifests, pending = _pending(MONGO_ENTITIES, batch_id)
    if not pending:
        return manifests

    client = mongo_client(tz_aware=True)
    try:
        database = mongo_database(client)
        extracted_at = datetime.now(timezone.utc)
        for entity in pending:
            step = plan[entity.key]
            query = {}
            if step["mode"] == "incremental" and step["watermark"] is not None:
                query = {entity.watermark_column: {"$gt": int(step["watermark"])}}
            cursor = (
                database[entity.origin]
                .find(query)
                .sort([(entity.watermark_column, 1), ("_id", 1)])
                .batch_size(BATCH_SIZE)
            )
            rows = [flatten_mongo(entity.name, document) for document in cursor]
            manifests[entity.key] = write_batch(
                entity, batch_id, step["mode"], step["watermark"], rows, extracted_at,
            )
    finally:
        client.close()
    return manifests


def manifests_for_batch(entities, batch_id):
    """Manifiestos de un lote existente, para reprocesarlo sin tocar las fuentes."""
    manifests = {}
    missing = []
    for entity in entities:
        manifest = existing_batch(entity, batch_id)
        if manifest is None:
            missing.append(entity.key)
        else:
            manifests[entity.key] = manifest
    if missing:
        raise FileNotFoundError(f"El lote {batch_id} no tiene archivos en S3 para: {', '.join(missing)}")
    return manifests
