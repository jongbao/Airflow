from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task 
import pendulum
import datetime

with DAG(
    dag_id='dags_python_with_xcom_eg2',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2024,8,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'
    
    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_ids='python_xcom_push_by_return')
        print('xcom_pull 메소드로 직접 찾은 리턴 값:' + value1)
    
    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print('함수 입력으로 받은 값:' + status) 

    
    python_xcom_push_by_return = xcom_push_result()
    xcom_pull_2(python_xcom_push_by_return)
    python_xcom_push_by_return >> xcom_pull_1()