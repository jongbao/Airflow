from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import task
from airflow.exceptions import AirflowException

with DAG(
    dag_id="dags_python_with_trigger_rule_eg2",
    schedule=None,
    start_date=pendulum.datetime(2024, 8, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    @task.branch(task_id='branching')
    def random_branch():
        import random
        item_list = ['A','B','C']
        selected_item = random.choice(item_list)
        if selected_item == 'A':
            return 'task_a' # 후행 task_id
        elif selected_item == 'B':
            return 'task_b'
        elif selected_item == 'C':
            return 'task_c'
    
    task_a = BashOperator(
        task_id='task_a',
        bash_command='echo upstream1'
    )

    @task(task_id='task_b')
    def task_b():
        raise print('정상 처리')
    
    @task(task_id='task_c')
    def task_c():
        raise print('정상 처리')

    @task(task_id='task_d', trigger_rule='non_skipped')
    def task_d():
        raise print('정상 처리')
    
    random_branch >> [task_a, task_b(), task_c()] >> task_d()
