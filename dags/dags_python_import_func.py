from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp # airflow에서는 plugins 폴더까지 path로 잡혀있기 때문에 common부터 입력해야 함 

with DAG(
    dag_id='dags_python_import_func',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2024,8,1,tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    task_get_sftp = PythonOperator(
        task_id='task_get_sftp',
        python_callable=get_sftp
    )