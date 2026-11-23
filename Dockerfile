FROM apache/airflow:2.9.3-python3.11

# Provider necessario para usar PostgresHook na DAG.
RUN pip install --no-cache-dir \
    "apache-airflow==2.9.3" \
    "apache-airflow-providers-postgres==5.11.0"
