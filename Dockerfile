FROM python:3.8.10-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED=1

ENV AIRFLOW_HOME=/app/airflow

# DBT
ENV DBT_DIR=$AIRFLOW_HOME/dbt_cgpdata
ENV DBT_TARGET_DIR=$DBT_DIR/target
ENV DBT_PROFILES_DIR=$DBT_DIR
ENV DBT_VERSION=1.2.6

WORKDIR $AIRFLOW_HOME

RUN apt-get update && apt-get install curl -y

COPY scripts scripts
RUN chmod +x scripts/entrypoint.sh
RUN chmod +x scripts/init_connections.sh

COPY pyproject.toml poetry.lock ./

RUN pip3 install --upgrade --no-cache-dir pip \
    && pip3 install poetry \
    && poetry install
    # --only main
