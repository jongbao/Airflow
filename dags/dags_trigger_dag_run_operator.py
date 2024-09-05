from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="dags_trigger_dag_run_operator",
    schedule='30 9 * * *',
    start_date=pendulum.datetime(2024, 8, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start!"'
    )
    
    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task', # 필수 
        trigger_dag_id='dags_python_operator', # 필수 (어떤 dag을 실행(trigger)할 것인지)
        trigger_run_id=None,
        execution_date='{{ data_interval_start }}', # 해당 파라미터에 값 주면 트리거 실행 시 매뉴얼로 실행된걸로 간주됨 (run_id=manual__{{execution_date}})
        reset_dag_run=True, # 이미 해당 trigger_run_id로 실행된 이력이 있는데 실행시킬 것인지
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None
    )

    start_task >> trigger_dag_task