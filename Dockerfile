FROM python:3.12-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED=1

ENV AIRFLOW_HOME=/app/airflow

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# DBT
ENV DBT_DIR=$AIRFLOW_HOME/dbt_cgpdata
ENV DBT_TARGET_DIR=$DBT_DIR/target
ENV DBT_PROFILES_DIR=$DBT_DIR
# ENV DBT_VERSION=1.2.6

WORKDIR $AIRFLOW_HOME

RUN apt-get update && apt-get install curl -y

COPY pyproject.toml poetry.lock ./

RUN pip3 install --upgrade --no-cache-dir pip
RUN pip3 install poetry

# RUN poetry install
    # --only main
RUN poetry install --no-root && rm -rf $POETRY_CACHE_DIR

ENV VIRTUAL_ENV=$AIRFLOW_HOME/.venv \
    PATH=$AIRFLOW_HOME"/.venv/bin:$PATH"

COPY src src
COPY scripts scripts
RUN chmod +x scripts/entrypoint.sh

CMD ["/bin/bash"]
