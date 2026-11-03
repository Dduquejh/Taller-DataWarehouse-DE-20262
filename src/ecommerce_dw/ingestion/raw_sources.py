"""Entidades extraídas: columnas, tipos en Parquet y Redshift, y marca de agua.

Cada entidad se escribe en un Parquet con exactamente las columnas de su
tabla raw en Redshift, en el mismo orden: COPY ... FORMAT AS PARQUET asocia
las columnas por posición. Las tres últimas son metadatos del lote.

Extracción incremental (marca de agua por entidad):

| Entidad            | Columna de avance | Motivo                                              |
|--------------------|-------------------|-----------------------------------------------------|
| customers/products | updated_at        | Cambian en la fuente; updated_at registra el cambio |
| orders/order_items | id                | Son inmutables; ids crecientes                      |
| reviews/events     | review_id/event_id| Documentos inmutables; ids crecientes               |

Una extracción completa (full) copia toda la entidad y redefine su estado en
staging; las incrementales solo traen filas con avance > marca de agua.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

import pyarrow as pa
from bson import json_util

BATCH_SIZE = 1000

# RAW es palabra reservada en Redshift: el esquema siempre va entre comillas.
RAW_SCHEMA = '"raw"'

META_COLUMNS = [
    ("_batch_id", pa.string(), "VARCHAR(32)"),
    ("_source", pa.string(), "VARCHAR(64)"),
    ("_extracted_at", pa.timestamp("us"), "TIMESTAMP"),
]


@dataclass(frozen=True)
class Entity:
    source: str          # mysql | documentdb (carpeta en S3)
    name: str            # customers, reviews, ...
    raw_table: str       # tabla en el esquema raw de Redshift
    columns: list        # [(nombre, tipo pyarrow, tipo Redshift)]
    watermark_column: str
    watermark_kind: str  # timestamp | integer
    origin: str = field(default="")  # tabla o colección de origen

    @property
    def key(self):
        return f"{self.source}.{self.name}"

    @property
    def source_label(self):
        prefix = "mysql" if self.source == "mysql" else "documentdb"
        return f"{prefix}.{self.origin or self.name}"

    @property
    def all_columns(self):
        return self.columns + META_COLUMNS

    def arrow_schema(self):
        return pa.schema([(name, kind) for name, kind, _ in self.all_columns])

    def redshift_ddl(self):
        columns = ",\n    ".join(f"{name} {kind}" for name, _, kind in self.all_columns)
        return f"CREATE TABLE IF NOT EXISTS {self.qualified_table} (\n    {columns}\n)"

    @property
    def qualified_table(self):
        return f"{RAW_SCHEMA}.{self.raw_table}"


DEC12 = pa.decimal128(12, 2)
DEC14 = pa.decimal128(14, 2)

MYSQL_ENTITIES = [
    Entity("mysql", "customers", "mysql_customers", [
        ("customer_id", pa.int64(), "BIGINT"),
        ("name", pa.string(), "VARCHAR(512)"),
        ("email", pa.string(), "VARCHAR(512)"),
        ("phone", pa.string(), "VARCHAR(64)"),
        ("city", pa.string(), "VARCHAR(256)"),
        ("department", pa.string(), "VARCHAR(256)"),
        ("registration_date", pa.date32(), "DATE"),
        ("updated_at", pa.timestamp("us"), "TIMESTAMP"),
    ], "updated_at", "timestamp", "customers"),
    Entity("mysql", "products", "mysql_products", [
        ("product_id", pa.int64(), "BIGINT"),
        ("name", pa.string(), "VARCHAR(512)"),
        ("category", pa.string(), "VARCHAR(256)"),
        ("price", DEC12, "DECIMAL(12,2)"),
        ("description", pa.string(), "VARCHAR(2048)"),
        ("is_active", pa.bool_(), "BOOLEAN"),
        ("updated_at", pa.timestamp("us"), "TIMESTAMP"),
    ], "updated_at", "timestamp", "products"),
    Entity("mysql", "orders", "mysql_orders", [
        ("order_id", pa.int64(), "BIGINT"),
        ("customer_id", pa.int64(), "BIGINT"),
        ("order_date", pa.timestamp("us"), "TIMESTAMP"),
        ("status", pa.string(), "VARCHAR(64)"),
        ("sales_channel", pa.string(), "VARCHAR(64)"),
        ("payment_method", pa.string(), "VARCHAR(64)"),
        ("total", DEC14, "DECIMAL(14,2)"),
    ], "order_id", "integer", "orders"),
    Entity("mysql", "order_items", "mysql_order_items", [
        ("order_item_id", pa.int64(), "BIGINT"),
        ("order_id", pa.int64(), "BIGINT"),
        ("product_id", pa.int64(), "BIGINT"),
        ("quantity", pa.int32(), "INTEGER"),
        ("unit_price", DEC12, "DECIMAL(12,2)"),
        ("subtotal", DEC14, "DECIMAL(14,2)"),
    ], "order_item_id", "integer", "order_items"),
]

# Las fechas de DocumentDB se guardan en UTC; en raw quedan como TIMESTAMP
# sin zona, en UTC. staging las convierte a hora de Bogotá.
MONGO_ENTITIES = [
    Entity("documentdb", "reviews", "mongo_reviews", [
        ("review_id", pa.int64(), "BIGINT"),
        ("customer_id", pa.int64(), "BIGINT"),
        ("product_id", pa.int64(), "BIGINT"),
        ("rating", pa.int32(), "INTEGER"),
        ("reviewed_at", pa.timestamp("us"), "TIMESTAMP"),
        ("document_json", pa.string(), "VARCHAR(65535)"),
    ], "review_id", "integer", "reviews"),
    Entity("documentdb", "navigation_events", "mongo_navigation_events", [
        ("event_id", pa.int64(), "BIGINT"),
        ("event_type", pa.string(), "VARCHAR(128)"),
        ("occurred_at", pa.timestamp("us"), "TIMESTAMP"),
        ("customer_id", pa.int64(), "BIGINT"),
        ("product_id", pa.int64(), "BIGINT"),
        ("sales_channel", pa.string(), "VARCHAR(64)"),
        ("document_json", pa.string(), "VARCHAR(65535)"),
    ], "event_id", "integer", "navigation_events"),
]

ENTITIES = MYSQL_ENTITIES + MONGO_ENTITIES
ENTITIES_BY_KEY = {entity.key: entity for entity in ENTITIES}


# ---------------------------------------------------------------- Marcas de agua

def parse_watermark(entity, value):
    if value is None:
        return None
    if entity.watermark_kind == "integer":
        return int(value)
    return datetime.fromisoformat(value)


def format_watermark(entity, value):
    if value is None:
        return None
    if entity.watermark_kind == "integer":
        return str(int(value))
    return value.isoformat(sep=" ")


# ---------------------------------------------------------------- Normalización

def _utc_naive(value):
    if value is None:
        return None
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _int(value):
    return None if value is None else int(value)


def normalize_value(kind, value):
    """Convierte un valor de la fuente al tipo de pyarrow de la columna."""
    if value is None:
        return None
    if pa.types.is_boolean(kind):
        return bool(value)
    if pa.types.is_integer(kind):
        return int(value)
    if pa.types.is_decimal(kind):
        return value if isinstance(value, Decimal) else Decimal(str(value))
    if pa.types.is_timestamp(kind):
        return _utc_naive(value) if isinstance(value, datetime) else value
    if pa.types.is_date(kind):
        return value.date() if isinstance(value, datetime) else value
    if pa.types.is_string(kind):
        return value if isinstance(value, str) else str(value)
    return value


def flatten_mongo(entity_name, document):
    """Convierte un documento en la fila de su tabla raw (sin metadatos)."""
    customer = document.get("customer") or {}
    product = document.get("product") or {}
    document_json = json_util.dumps(document, json_options=json_util.RELAXED_JSON_OPTIONS, ensure_ascii=False)
    if entity_name == "reviews":
        return (
            _int(document.get("review_id")), _int(customer.get("customer_id")),
            _int(product.get("product_id")), _int(document.get("rating")),
            _utc_naive(document.get("reviewed_at")), document_json,
        )
    device = document.get("device") or {}
    return (
        _int(document.get("event_id")), document.get("event_type"),
        _utc_naive(document.get("occurred_at")), _int(customer.get("customer_id")),
        _int(product.get("product_id")), device.get("sales_channel"), document_json,
    )

