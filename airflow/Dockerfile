FROM apache/airflow:2.10.5-python3.11

COPY --chown=airflow:root pyproject.toml /opt/airflow/project/pyproject.toml
COPY --chown=airflow:root ingestion /opt/airflow/project/ingestion
COPY --chown=airflow:root quality /opt/airflow/project/quality
COPY --chown=airflow:root warehouse /opt/airflow/project/warehouse

WORKDIR /opt/airflow/project
RUN pip install --no-cache-dir ".[ingestion,quality,warehouse]"

WORKDIR /opt/airflow
