import airflow
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
import gzip
import requests
import psycopg2
import logging
from datetime import datetime, timedelta
from json import loads
from gzip import decompress
from requests import get
from psycopg2 import extras

param_dic = {
    "host"      : "postgres",
    "database"  : "airflow_db",
    "user"      : "airflow",
    "password"  : "airflow"
}

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
        sys.exit(1) 
    logging.info("Connection successful")
    return conn

def _store_pronostico(ti):
    conn = connect(param_dic)
    cursor = conn.cursor()

    pronostico = ti.xcom_pull(task_ids="extract_pronostico")
    for pronostico_fact in pronostico:
        insert_query = "insert into pronosticoxmunicipios_" + timest.strftime("%m_%d_%Y_%H") + " VALUES(%(ides)s, %(idmun)s, %(nes)s, %(nmun)s, %(hloc)s, %(dsem)s, %(nhor)s, %(temp)s, %(desciel)s, %(probprec)s, %(prec)s, %(velvien)s, %(dirvienc)s, %(dirvieng)s, %(hr)s, %(lat)s, %(lon)s, %(dpt)s, %(dh)s, %(raf)s, '" + timest.strftime("%m/%d/%Y, %H:%M:%S") + "')"
        cursor.execute(insert_query, pronostico_fact)
        conn.commit()
    conn.close()

def extract_json():
    logging.info("Decompressing .gz file")
    return loads(decompress(get("https://smn.conagua.gob.mx/webservices/?method=3", verify=False).content))

def _get_average_pronostico():
    conn = connect(param_dic)
    cursor = conn.cursor()
    select_query = ""
    if bool(cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='"+ (timest - timedelta(hours=1)).strftime("%m_%d_%Y_%H") +"')")):
        logging.info('Previous hr table exists. Moving On.')
        select_query = '''SELECT id_es, id_mun, avg(temp) as avg_temp , avg(prec) as avg_prec INTO pronostico_avg_''' + timest.strftime("%m_%d_%Y_%H") + ''' FROM ( 
                SELECT id_es, id_mun, temp, prec 
                FROM pronosticoxmunicipios_09_12_2022_18 
                UNION ALL  
                SELECT id_es, id_mun, temp , prec
                FROM pronosticoxmunicipios_09_12_2022_19 ) sub
            GROUP BY id_es, id_mun;'''
    else:
        logging.info('Previous hr does not exist. Creating the Table now.')
        select_query = "SELECT id_es, id_mun, avg(temp) as avg_temp , avg(prec) as avg_prec INTO pronostico_avg_" + timest.strftime("%m_%d_%Y_%H") + " FROM pronosticoxmunicipios_" + timest.strftime("%m_%d_%Y_%H") + " GROUP BY id_es, id_mun;"
    conn.close()
    logging.info(select_query)
    return select_query

def csvToPostgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    # CSV loading to table.
    with open('/opt/airflow/data_municipios/20220503/data.csv', 'r') as f:
        next(f)
        curr.copy_from(f, 'data_municipios_20220503', sep=',')
        get_postgres_conn.commit()
 
with DAG('pronostico_processing', start_date=datetime(2022, 9, 9), 
        schedule_interval='@hourly', catchup=False) as dag:

    timest = datetime.now()
 
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS pronosticoxmunicipios_''' + timest.strftime("%m_%d_%Y_%H") +  ''' (
                id_es INT NOT NULL,
                id_mun INT NOT NULL,
                nom_es TEXT NOT NULL,
                nom_mun TEXT NOT NULL,
                h_loc TEXT NOT NULL,
                d_sem TEXT NOT NULL,
                nhor INT NOT NULL, 
                temp FLOAT NOT NULL,
                desc_ciel TEXT NOT NULL,
                prob_prec FLOAT NOT NULL,
                prec FLOAT NOT NULL,
                vel_vien FLOAT NOT NULL,
                dir_vien_c TEXT NOT NULL,
                dir_vien_g FLOAT NOT NULL,
                hum_rel FLOAT NOT NULL,
                lat FLOAT NOT NULL,
                lon FLOAT NOT NULL,
                dpt FLOAT NOT NULL,
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

    get_average_pronostico = PythonOperator(
        task_id='get_average_pronostico',
        python_callable=_get_average_pronostico
    )

    execute_average_pronostico = PostgresOperator(
        task_id='execute_average_pronostico',
        postgres_conn_id='postgres',
        sql='''
            {{ ti.xcom_pull(task_ids='get_average_pronostico') }}
        '''
    )

    # This operator was created to retrieve the most recent folder from data_municipios
    get_last_data_municipios = DummyOperator(
        task_id='get_data_municipios',
        #bash_command='find ../../opt/airflow/data_municipios ! -path . -type d | sort -nr | head -1',
    )

    create_data_municipios_table = PostgresOperator(
        task_id='create_data_municipios_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS data_municipios_20220503 ( 
                id_es INT NOT NULL,
                id_mun INT NOT NULL,
                value_ TEXT NOT NULL
            )
            '''
    ) 

    process_data_municipios = PythonOperator(
        task_id="process_data_municipios",
        python_callable=csvToPostgres,
    )

    merge_data_pronostico_mun = PostgresOperator(
        task_id='merge_data_pronostico_mun',
        postgres_conn_id='postgres',
        sql=['''
            SELECT ides, idmun, avg_temp, avg_prec, value_ INTO data_pronostico_mun_''' + timest.strftime("%m_%d_%Y_%H") + '''
            FROM (
                SELECT pm.id_es as ides, pm.id_mun as idmun, avg_temp, avg_prec, value_ FROM pronostico_avg_''' + timest.strftime("%m_%d_%Y_%H") +  ''' pm
                JOIN data_municipios_20220503 dm ON dm.id_es = pm.id_es AND dm.id_mun = pm.id_mun
            ) sub
            ''',
            ''' TRUNCATE data_pronostico_mun_current; ''',
            ''' INSERT INTO data_pronostico_mun_current SELECT * FROM data_pronostico_mun_''' + timest.strftime("%m_%d_%Y_%H") + ''';'''
            ]
    )

    create_table >> extract_pronostico >> store_pronostico >> get_average_pronostico >> execute_average_pronostico >> get_last_data_municipios >> create_data_municipios_table >> process_data_municipios >> merge_data_pronostico_mun