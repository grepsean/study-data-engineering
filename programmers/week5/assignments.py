from airflow import DAG
from airflow.macros import *
from airflow.models import Variable
from airflow.operators.sql import SQLCheckOperator
from plugins.postgres_to_s3_operator import PostgresToS3Operator
from plugins.s3_to_redshift_operator import S3ToRedshiftOperator

DAG_ID = "Postgres_to_Redshift"
dag = DAG(
    DAG_ID,
    schedule_interval="@once",
    max_active_runs=1,
    concurrency=2,
    start_date=datetime(2020, 11, 5),
    catchup=False
)

tables = [
    "customer_features",
    "customer_variants",
    "customer_interactions",
    "item_features",
]

# s3_bucket, local_dir
s3_bucket = Variable.get('s3_bucket')
local_dir = Variable.get('s3_temp_dir')  # 실제 프로덕션에서는 공간이 충분한 폴더 (volume)로 맞춰준다
s3_key_prefix = Variable.get('s3_key_prefix')  # 본인의 ID에 맞게 수정
redshift_schema = 'seankkang'  # 본인이 사용하는 스키마에 맞게 수정

prev_task = None

for table in tables:
    s3_key = f'{s3_key_prefix}/{table}.tsv'

    postgrestos3 = PostgresToS3Operator(
        table=f'public.{table}',
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        data_dir=local_dir,
        dag=dag,
        task_id=f'Postgres_to_S3_{table}'
    )

    s3toredshift = S3ToRedshiftOperator(
        schema=redshift_schema,
        table=table,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        copy_options="delimiter '\\t' COMPUPDATE ON",
        aws_conn_id='aws_s3_default',
        task_id=f'S3_to_Redshift_{table}',
        dag=dag
    )

    sqlCheck = SQLCheckOperator(
        task_id=f'Sql_Check_{table}',
        sql=f'SELECT COUNT(1) FROM {redshift_schema}.{table}',
        conn_id='redshift_dev_db'
    )

    postgrestos3 >> s3toredshift >> sqlCheck
