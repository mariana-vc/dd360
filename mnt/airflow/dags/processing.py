import airflow
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
import gzip
import requests
import psycopg2
import logging
from pandas import json_normalize
from datetime import datetime
from json import loads
from gzip import decompress
from requests import get
from psycopg2 import extras

def _store_pronostico(ti):

    conn = psycopg2.connect(user="airflow",
                                password="airflow",
                                host="postgres",
                                port="5432",
                                database="airflow_db")

    cursor = conn.cursor()

    pronostico = ti.xcom_pull(task_ids="extract_pronostico")
    timest = datetime.now()
    for pronostico_fact in pronostico:
        insert_query = "insert into pronosticoxmunicipios VALUES(%(ides)s, %(idmun)s, %(nes)s, %(nmun)s, %(dloc)s, %(ndia)s, %(tmax)s, %(tmin)s, %(desciel)s, %(probprec)s, %(prec)s, %(velvien)s, %(dirvienc)s, %(dirvieng)s, %(cc)s, %(lat)s, %(lon)s, %(dh)s, %(raf)s, '" + timest.strftime("%m/%d/%Y, %H:%M:%S") + "')"
        cursor.execute(insert_query, pronostico_fact)
        conn.commit()
    conn.close()

def extract_json():
    return loads(decompress(get("https://smn.conagua.gob.mx/webservices/?method=1", verify=False).content))
 
with DAG('pronostico_processing', start_date=datetime(2022, 9, 9), 
        schedule_interval='@hourly', catchup=False) as dag:
 
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS pronosticoxmunicipios (
                id_es INT NOT NULL,
                id_mun INT NOT NULL,
                nom_es TEXT NOT NULL,
                nom_mun TEXT NOT NULL,
                dia_loc TEXT NOT NULL,
                num_dia INT NOT NULL,
                temp_max FLOAT NOT NULL,
                temp_min FLOAT NOT NULL,
                desc_ciel TEXT NOT NULL,
                prob_prec FLOAT NOT NULL,
                prec FLOAT NOT NULL,
                vel_vien FLOAT NOT NULL,
                dir_vien_c TEXT NOT NULL,
                dir_vien_g FLOAT NOT NULL,
                cc FLOAT NOT NULL,
                lat FLOAT NOT NULL,
                lon FLOAT NOT NULL,
                dh INT NOT NULL,
                raf FLOAT NOT NULL,
                tms TEXT NOT NULL
            );
        '''
    )
 
    extract_pronostico = PythonOperator(
        task_id='extract_pronostico',
        python_callable=extract_json
    )
 
    store_pronostico = PythonOperator(
        task_id='store_pronostico',
        python_callable=_store_pronostico
    )
 
 
    create_table >> extract_pronostico >> store_pronostico 