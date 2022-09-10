import airflow
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
import psycopg2
from pandas import json_normalize
from datetime import datetime
 
def _process_pronostico(ti):

    conn = psycopg2.connect(user="airflow",
                                password="airflow",
                                host="postgres",
                                port="5432",
                                database="airflow_db")

    cursor = conn.cursor()

    pronostico = ti.xcom_pull(task_ids="extract_pronostico")
    pronostico_json = pronostico[0]
    for pronostico_fact in pronostico_json:
        data = json.dumps(pronostico_fact)
        insert_query = "insert into pronosticoPorMunicipios (data) values (%s) returning data"
        cursor.execute(insert_query, (data,))
    conn.commit()
    conn.close()
    '''processed_pronostico = json_normalize({
        'firstname': pronostico['name'],
        'lastname': pronostico['name'],
        'country': pronostico['location'],
        'pronosticoname': pronostico['login'],
        'password': user['login'],
        'email': user['email'] })
    processed_pronostico.to_csv('/tmp/processed_pronostico.csv', index=None, header=False)'''
 
def _store_pronostico():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )
 
with DAG('user_processing', start_date=datetime(2022, 9, 9), 
        schedule_interval='@hourly', catchup=False) as dag:
 
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS pronosticoPorMunicipios (
                id_es INT NOT NULL,
                id_mun INT NOT NULL,
                nom_es TEXT NOT NULL,
                nom_mun TEXT NOT NULL,
                dia_loc TEXT NOT NULL,
                num_dia INT NOT NULL,
                temp_max INT NOT NULL,
                temp_min INT NOT NULL,
                desc_ciel TEXT NOT NULL,
                prob_prec INT NOT NULL,
                prec INT NOT NULL,
                vel_vien INT NOT NULL,
                dir_vien_c INT NOT NULL,
                dir_vien_g INT NOT NULL,
                cc INT NOT NULL,
                lat INT NOT NULL,
                lon INT NOT NULL,
                dh INT NOT NULL
            );
        '''
    )
 
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='pronostico_api',
        endpoint='/webservices',
        request_params={"method":"1"},
        extra_options = {'verify':False}
    )
 
    extract_pronostico = SimpleHttpOperator(
        task_id='extract_pronostico',
        http_conn_id='pronostico_api',
        endpoint='/webservices/?method=1',
        method='GET',
        headers={"Content-Type": "application/json"},
        #response_filter=lambda response: response.json()[''],
        response_filter=lambda response: json.loads((response.text).decode("utf-8")),
        log_response=True,
        extra_options = {'verify':False} 
    )
 
    process_pronostico = PythonOperator(
        task_id='process_pronostico',
        python_callable=_process_pronostico
    )
 
    #store_pronostico = PythonOperator(
    #    task_id='store_pronostico',
    #    python_callable=_store_pronostico
    #)
 
    create_table >> is_api_available >> extract_pronostico >> process_pronostico 