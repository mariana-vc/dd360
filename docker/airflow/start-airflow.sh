#!/usr/bin/env bash

# Move to the AIRFLOW HOME directory
cd $AIRFLOW_HOME

# Export environement variables
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Initiliase the metadatabase
airflow db init

# Create User
airflow users create -e "admin@airflow.com" -f "airflow" -l "airflow" -p "airflow" -r "Admin" -u "airflow"

# Run the scheduler in background
airflow scheduler &> /dev/null &

airflow connections add 'postgres' \
    --conn-type 'Postgres' \
    --conn-host 'postgres' \
    --conn-schema 'airflow_db' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-port '5432'

airflow connections add 'pronostico_api' \
    --conn-type 'HTTP' \
    --conn-host 'https://smn.conagua.gob.mx/'
    

# Run the web sever in foreground (for docker logs)
exec airflow webserver