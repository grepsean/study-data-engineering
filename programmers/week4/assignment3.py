import logging
from datetime import datetime

import psycopg2
import requests
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.exceptions import AirflowException


def get_Redshift_connection():
    host = "..."
    redshift_user = "..."
    redshift_pass = "..."
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()


def execute_with_log(cursor, sql):
    logging.info(f'Execute : {sql};')
    cursor.execute(sql)


def extract(url, **kwargs):
    f = requests.get(url)
    if not f or not f.text or not f.text.strip():
        raise AirflowException("extract task fail!")

    kwargs['task_instance'].xcom_push(key='extracted_data', value=f.text)
    return f.text


def transform(text, **kwargs):
    lines = text.strip().split("\n")
    if not lines:
        raise AirflowException("transform task fail!")

    kwargs['task_instance'].xcom_push(key='transformed_data', value=lines)
    return lines


def load(target_table, *lines, **kwargs):
    cur = get_Redshift_connection()

    # Start transaction
    execute_with_log(cur, f'BEGIN')
    # Truncate table before Insert
    execute_with_log(cur, f'TRUNCATE TABLE {target_table}')

    for record in lines[1:]:  # except header
        if record and record.strip():
            name, gender = record.split(',')
            if name and gender:
                execute_with_log(cur, f"INSERT INTO {target_table} VALUES ('{name}', '{gender}')")

    # End transaction
    execute_with_log(cur, f'END')


with DAG(
        dag_id='import_csv',
        start_date=datetime(2020, 11, 1),
        schedule_interval='0 2 * * *'
) as dag:
    extract_task = PythonOperator(
        task_id='extract_task',
        provide_context=True,
        python_callable=extract,
        op_args=('https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv',),
        dag=dag)

    transform_task = PythonOperator(
        task_id='transform_task',
        provide_context=True,
        python_callable=transform,
        op_args=('{{ task_instance.xcom_pull(task_ids=\'extract_task\', key=\'extracted_data\') }}',),
        dag=dag)

    load_task = PythonOperator(
        task_id='load_task',
        provide_context=True,
        python_callable=load,
        op_args=('seankkang.name_gender',
                 '{{ task_instance.xcom_pull(task_ids=\'transform_task\', key=\'transformed_data\') }}',),
        dag=dag)

    # Assign the order of the tasks in our DAG
    extract_task >> transform_task >> load_task
