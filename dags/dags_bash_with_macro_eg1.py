from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task 
import pendulum
import datetime

with DAG(
    dag_id='dags_bash_with_macro_eg1',
    schedule='10 0 L * *', # 매 월 말일 0시 10분
    start_date=pendulum.datetime(2024,8,1,tz='Asia/Seoul'),
    catchup=False
) as dag:
    # START_DATE: 전월 말일, END_DATE: 1일 전
    bash_task1 = BashOperator(
        task_id='bash_task_1',
        # 템플릿에서 꺼내 쓰는 날짜 변수들은 타임 존이 기본적으로 utc(한국보다 9시간 느림) 이므로 한국 시간으로 바꿔야 함
        env={'START_DATE':'{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}', # data_interval_strat = 이전 배치에 돌았던 날짜이므로 전월 말일이 됨
             'END_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds }}'},
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )