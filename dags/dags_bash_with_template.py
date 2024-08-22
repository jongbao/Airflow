from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_template",
    schedule="10 0 * * *", # 0시 10분 첫번째주 토요일
    start_date=pendulum.datetime(2024,8,1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    bash_t1 = BashOperator(
        task_id='bash_t1',
        bash_command='echo "End date is {{ data_interval_end }}"' # 실제 DAG이 돌아가는 시간에 맞춰 나옴
    )

    bash_t2 = BashOperator(
        task_id='bash_t2',
        # 환경변수 설정
        env={'START_DATE': '{{ data_interval_start | ds }}', # | ds -> yyyy-mm-dd 형태로 출력 가능
                'END_DATE': '{{ data_interval_end | ds }}'},
        bash_command='echo "Start date is $START_DATE" && ' # && 앞에 있는 command가 성공하면 뒤에 command 실행하겠다
                                'echo "End date is $END_DATE"'
    )

    bash_t1 >> bash_t2