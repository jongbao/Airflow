from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="10 0 * * 6#1", # 0시 10분 첫번째주 토요일
    start_date=pendulum.datetime(2024,8,1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    t1_orange = BashOperator(
        task_id = "t1_orange",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE" 
        # 태스크를 실행하는 주체는 워커 컨테이너이기 때문에 컨테이너가 인식할 수 있는 경로로 표시해야 함
        # 리눅스 환경 - 컨테이너 volume mount 하는 작업을 해야 가능함
    )


    t2_avocado = BashOperator(
        task_id = "t1_avocado",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh AVOCADO"
    )

    t1_orange >> t2_avocado