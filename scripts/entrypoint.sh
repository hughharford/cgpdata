#!/usr/bin/env bash

airflow db upgrade

airflow users create -r Admin -u admin -p admin -e admin@example.com -f admin -l airflow

# scripts/init_connections.sh

airflow webserver
