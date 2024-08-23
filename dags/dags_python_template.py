from airflow import DAG
from airflow.decorators import task 
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import pendulum
import datetime

with DAG(
    dag_id='dags_python_template',
    schedule='30 9 * * *',
    start_date=pendulum.datetime(2024,8,1,tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    # 만약 start_date, end_date 인수 안 받고 싶으면
    # op_kwargs로 전달되고 해당 딕셔너리에서 key값에 맞는 value 꺼내서 할당
    # def python_function1(**kwargs):
    #     start_date = kwargs['start_date']
    #     end_date = kwargs['end_date']
    #     print(start_date)
    #     print(end_date)

    # 해당 코드로 작성하면 start_date, end_date와 이름이 맞는 인수를 찾아 바로 할당
    def python_function1(start_date, end_date):
        print(start_date)
        print(end_date)
    
    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable=python_function1,
        op_kwargs={'start_date':'{{ data_interval_start | ds }}', 'end_date':'{{ data_interval_end | ds }}'}
    )


    # @task(task_id='python_t2')
    # def python_function2(**kwargs):
    #     print(**kwargs)
    #     print('ds:' + kwargs['ds'])
    #     print('ts:' + kwargs['ts'])
    #     print('data_interval_start:' + str(kwargs['data_interval_start']))
    #     print('data_interval_end:' + str(kwargs['data_interval_end']))
    #     print('task_instance:' + str(kwargs['ti']))

    python_t1
    # python_t1 >> python_function2