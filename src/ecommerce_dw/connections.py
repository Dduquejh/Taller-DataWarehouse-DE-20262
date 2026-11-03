"""Conexiones a las fuentes (RDS MySQL, DocumentDB), Redshift y S3.

En AWS las credenciales no están en variables de entorno: se leen de Secrets
Manager con el rol de la instancia (MYSQL_SECRET_ARN, DOCDB_SECRET_ARN,
REDSHIFT_SECRET_ARN). Si un ARN no está definido se usan MYSQL_USER /
MYSQL_PASSWORD, MONGODB_URI, etc., lo que permite ejecutar los scripts contra
bases locales.

RDS y DocumentDB exigen TLS; TLS_CA_FILE apunta al certificado raíz de AWS
(global-bundle.pem) que descarga user_data.
"""

import json
import os
from functools import lru_cache
from pathlib import Path

DEFAULT_REGION = "us-east-2"


def region():
    return os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or DEFAULT_REGION


def _ca_file():
    path = os.getenv("TLS_CA_FILE")
    return path if path and Path(path).is_file() else None


@lru_cache(maxsize=None)
def secret(arn):
    """Devuelve el JSON de un secreto de Secrets Manager (cacheado por proceso)."""
    import boto3

    client = boto3.client("secretsmanager", region_name=region())
    return json.loads(client.get_secret_value(SecretId=arn)["SecretString"])


def credentials(secret_env, user_env, password_env, default_user=None):
    """Usuario y contraseña desde Secrets Manager o, si no hay ARN, desde el entorno."""
    arn = os.getenv(secret_env)
    if arn:
        values = secret(arn)
        return values["username"], values["password"]
    return os.getenv(user_env, default_user), os.environ[password_env]


# ---------------------------------------------------------------- MySQL (RDS)

def mysql_connection(**kwargs):
    import pymysql

    user, password = credentials("MYSQL_SECRET_ARN", "MYSQL_USER", "MYSQL_PASSWORD")
    ca = _ca_file()
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=user,
        password=password,
        database=os.getenv("MYSQL_DATABASE", "ecommerce"),
        charset="utf8mb4",
        connect_timeout=10,
        ssl={"ca": ca} if ca else None,
        **kwargs,
    )


# ---------------------------------------------------------------- DocumentDB

def mongo_client(**kwargs):
    """Cliente de DocumentDB (o de MongoDB si se define MONGODB_URI).

    Diferencias con MongoDB que se ajustan aquí:
    - DocumentDB no admite retryable writes: retryWrites=False.
    - TLS obligatorio con el certificado de AWS.
    - El endpoint del clúster con replicaSet=rs0 dirige las escrituras al primario.
    """
    from pymongo import MongoClient

    if not os.getenv("DOCDB_SECRET_ARN") and os.getenv("MONGODB_URI"):
        return MongoClient(os.environ["MONGODB_URI"], **kwargs)

    user, password = credentials("DOCDB_SECRET_ARN", "MONGO_ROOT_USERNAME", "MONGO_ROOT_PASSWORD")
    ca = _ca_file()
    options = dict(
        host=os.environ["DOCDB_HOST"],
        port=int(os.getenv("DOCDB_PORT", "27017")),
        username=user,
        password=password,
        authSource="admin",
        replicaSet="rs0",
        retryWrites=False,
        serverSelectionTimeoutMS=10_000,
        tls=True,
    )
    if ca:
        options["tlsCAFile"] = ca
    options.update(kwargs)
    return MongoClient(**options)


def mongo_database(client):
    return client[os.getenv("MONGO_DATABASE", "ecommerce")]


# ---------------------------------------------------------------- Redshift

def redshift_credentials():
    return credentials("REDSHIFT_SECRET_ARN", "REDSHIFT_USER", "REDSHIFT_PASSWORD")


def redshift_connection():
    import psycopg2

    user, password = redshift_credentials()
    return psycopg2.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
        dbname=os.getenv("REDSHIFT_DATABASE", "ecommerce_dw"),
        user=user,
        password=password,
        sslmode="require",
        connect_timeout=15,
        application_name="ecommerce_dw_pipeline",
    )


# ---------------------------------------------------------------- S3

def s3_client():
    import boto3

    return boto3.client("s3", region_name=region())


def bucket():
    return os.environ["S3_BUCKET"]
