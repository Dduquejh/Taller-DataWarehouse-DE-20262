# Data warehouse de comercio electrónico en AWS

Migración a AWS del data warehouse local (DuckDB) del repositorio DE-DW. Se
conservan las reglas de negocio, el modelo dimensional, la cuarentena y el
SCD tipo 2. El pipeline:

1. Extrae de **RDS MySQL** y **DocumentDB** a Parquet en un bucket **S3** privado, por lote.
2. Carga con `COPY` a **Redshift Serverless**.
3. Transforma y prueba con **dbt**, orquestado por **Airflow**. Airflow y dbt corren en la misma instancia **EC2**.
4. Publica una vista para **QuickSight**.

| Documento | Contenido |
|---|---|
| [terraform/README.md](terraform/README.md) | Infraestructura: diagrama, red y permisos, despliegue, acceso, costos y eliminación |
| [docs/aws/pipeline.md](docs/aws/pipeline.md) | Lotes, marcas de agua, estados, escenarios de evidencia, QuickSight y diferencias frente a la versión local |
| [docs/data_model.md](docs/data_model.md) | Modelo estrella, SCD2 y llaves (igual que en local) |
| [docs/architecture.md](docs/architecture.md), [docs/etapas/](docs/etapas/) | Guía original de la versión local con DuckDB, como referencia |

## Despliegue

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # perfil, rama, IP
terraform init && terraform apply
```

La instancia clona este repositorio, genera `.env` desde SSM y levanta
Airflow. El arranque tarda entre 5 y 10 minutos y se sigue con
`sudo tail -f /var/log/cloud-init-output.log`.

## Ejecución

En la instancia (`eval "$(terraform output -raw ssm_session)"`):

```bash
cd /opt/ecommerce-dw
scripts/generate_data.sh                 # tablas, colecciones y datos sintéticos en RDS y DocumentDB
scripts/run_pipeline.sh --mode full      # primer lote: carga completa
scripts/warehouse.sh reconcile           # hechos vs. fuente validada
```

| Objetivo | Comando |
|---|---|
| Lote incremental | `scripts/run_pipeline.sh` |
| Reprocesar un lote desde S3 | `scripts/run_pipeline.sh --reprocess 20261103T110000Z` |
| Nuevo lote diario en las fuentes | `scripts/append_batch.sh --batch-id N` |
| Cambio de atributos (SCD2) | `scripts/simulate_changes.sh` |
| Evidencia incremental e idempotencia | `scripts/verify_incremental_load.sh --batch-id N` |
| Lotes, ejecuciones, cuarentena, SCD2 | `scripts/warehouse.sh batches \| runs \| quarantine \| scd2` |
| Consulta libre en Redshift | `scripts/warehouse.sh query "select ..."` |
| dbt fuera del DAG | `scripts/run_dbt.sh test` |
| Levantar o detener Airflow | `scripts/start_services.sh`, `scripts/stop_services.sh` |

El UI de Airflow se abre con el túnel
`eval "$(terraform output -raw ssm_tunnel_airflow)"` en
`http://localhost:8080`, con el usuario `airflow`. La contraseña está en
`.airflow-secrets` de la instancia.

## Estructura

```
terraform/                    infraestructura (VPC, S3, RDS, DocumentDB, Redshift, EC2, IAM, QuickSight)
docker/                       imagen de Airflow + dbt-redshift y compose para EC2
src/airflow/dags/             DAG ecommerce_pipeline
src/ecommerce_dw/
  connections.py              RDS, DocumentDB, Redshift y S3 (Secrets Manager + TLS)
  ingestion/                  entidades, extracción a S3, COPY a Redshift, tareas del DAG
  transformations/            ejecución de dbt
  sources/                    generación de datos y cambios en las fuentes
  evidence.py                 consultas de evidencia
src/dbt/                      proyecto dbt adaptado a Redshift
scripts/                      operación en la instancia
```

## Seguridad

- No hay secretos en el repositorio: las contraseñas de las bases las genera y guarda Secrets Manager, y las de Airflow se generan en la instancia.
- Las bases y Redshift son privados, con TLS obligatorio. Los security groups solo admiten conexiones desde Airflow (y desde QuickSight hacia Redshift).
- La instancia usa su rol IAM por IMDSv2. Redshift lee S3 con un rol de solo lectura sobre `raw/`.
- El bucket tiene bloqueo de acceso público, cifrado, versionado y TLS obligatorio, y el pipeline no puede borrar en `raw/`.

Ver [docs/aws/pipeline.md](docs/aws/pipeline.md) para los detalles.
