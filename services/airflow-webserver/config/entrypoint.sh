#!/bin/sh

. /opt/airflow/config/etl.cfg

airflow initdb

airflow create_user \
    --role Admin \
    --username airflow \
    --password airflow \
    --email airflow@airflow.com \
    --firstname airflow \
    --lastname airflow

airflow connections \
    --add \
    --conn_id aws_credentials \
    --conn_type aws \
    --conn_login $ACCESS_KEY_ID \
    --conn_password $SECRET_ACCESS_KEY

airflow connections \
    --add \
    --conn_id redshift \
    --conn_type postgres \
    --conn_login $REDSHIFT_LOGIN \
    --conn_password $REDSHIFT_PASSWORD \
    --conn_host $REDSHIFT_HOST \
    --conn_port $REDSHIFT_PORT \
    --conn_schema $REDSHIFT_SCHEMA
