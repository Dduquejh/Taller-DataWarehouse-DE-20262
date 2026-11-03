FROM apache/airflow:2.11.2-python3.11

ARG AIRFLOW_VERSION=2.11.2

# Dependencias del pipeline en el entorno de Airflow, fijadas por las
# constraints oficiales (boto3, psycopg2 y pyarrow incluidos).
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt" \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-amazon" \
    "Faker==37.0.0" \
    "PyMySQL==1.1.2" \
    "pymongo==4.16.0" \
    "pyarrow" \
    "boto3" \
    "psycopg2-binary" \
    && pip check

# dbt va en un venv aparte para no chocar con las dependencias de Airflow.
# Se conecta a Redshift: el SQL se ejecuta allá, no en la instancia.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
      "dbt-core~=1.9.4" \
      "dbt-redshift~=1.9.0" \
    && /home/airflow/dbt-venv/bin/pip check \
    && ln -s /home/airflow/dbt-venv/bin/dbt /home/airflow/.local/bin/dbt
