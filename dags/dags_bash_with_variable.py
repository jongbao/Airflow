from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *", # 0시 10분 첫번째주 토요일
    start_date=pendulum.datetime(2024,8,1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    # 1. Variable 라이브러리 사용 (권고x)
    var_value = Variable.get('sample_key')

    bash_var_1 = BashOperator(
        task_id='bash_var_1',
        bash_command=f'echo variable: {var_value}'
    )

    # 2. 템플릿 변수 사용 (var.value.~~~ 형태로 작성)
    bash_var_2 = BashOperator(
        task_id = 'bash_var_2',
        bash_command="echo variable: {{var.value.sample_key}}"
    )
