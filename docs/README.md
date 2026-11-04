# Documentación

Para replicar el proyecto, sigue el [README principal](../README.md).

## Versión en AWS (vigente)

| Documento | Contenido |
|---|---|
| [README principal](../README.md) | Despliegue, ejecución, validación, dashboard y eliminación de los recursos, paso a paso |
| [aws/pipeline.md](aws/pipeline.md) | Lotes, marcas de agua, estados, recuperación, decisiones técnicas y diferencias frente a la versión local |
| [terraform/README.md](../terraform/README.md) | Red, permisos, acceso, túneles, pausa de los recursos y costos |

## Modelo de datos

[data_model.md](data_model.md) describe el modelo estrella, el SCD tipo 2 y las
llaves. El modelo es el mismo en AWS, pero el documento habla de DuckDB porque
viene de la versión local. Las capas equivalentes en Redshift están en
[aws/pipeline.md](aws/pipeline.md).

## Versión local con DuckDB (solo como referencia)

Estos documentos vienen del repositorio original y explican las reglas de negocio
y los conceptos de cada etapa. **No sirven para ejecutar este repositorio:** usan
DuckDB, MySQL y MongoDB en Docker, y mencionan archivos y scripts que ya no existen,
como `configure_environment.sh`, `start_infrastructure.sh` y los DAGs
`extract_to_raw` y `transform_warehouse`.

| Documento | Contenido |
|---|---|
| [architecture.md](architecture.md) | Arquitectura local, reglas de calidad y cuarentena |
| [airflow.md](airflow.md) | Conceptos de Airflow y DAGs de la versión local |
| [connections.md](connections.md) | Conexión a las bases locales |
| [analytical_queries.sql](analytical_queries.sql) | Consultas analíticas de ejemplo de la versión local, sin probar en Redshift |
| [etapas/](etapas/) | Guía por etapas, de la 01 a la 09 |
