from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import regist2

with DAG(
    dag_id='dags_python_with_op_kwargs',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2024,8,1,tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    regist2_t1 = PythonOperator(
        task_id='regist2_t1',
        python_callable=regist2,
        op_args=['jmjeon','woman','korea','seoul'],
        op_kwargs={'phone'='010','email':'whdala1120@naver.com'}
    )

    regist2_t1