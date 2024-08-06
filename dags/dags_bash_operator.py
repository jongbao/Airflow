from airflow import DAG
import datetime
import pendulum

from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator", # dag 파일명과 id는 일치시키는 것이 좋음
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False, # start_date부터 현재까지 누락된 구간에 대해 실행시킬 건지 (True면 한꺼번에 돌리는데 문제될 수 있음)
    # dagrun_timeout=datetime.timedelta(minutes=60), # DAG이 60분 이상 돌면 실패
    # tags=["example", "example2"], # 옵션
    # params={"example_key": "example_value"}, # Task에 넘겨줄 파라미터
) as dag:
    # task 객체 명
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami", # ehco = print
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME", # ehco = print
    )

    # task들의 수행 순서
    bash_t1 >> bash_t2