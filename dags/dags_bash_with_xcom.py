from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2024,8,1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    bash_push = BashOperator(
        task_id='bash_push',
        bash_command="echo START &&"
                    "echo XOM_PUSHED"
                    "{{ ti.xcom_push(key='bash_pushed', value='first_bash_message' )}} &&"
                    "echo COMPLETE" # 제일 마지막 echo가 return으로 간주 됨
    )

    bash_pull = BashOperator(
        task_id='bash_pull',
        env={'PUSHED_VALUE':"{{ ti.xcom_pull(key='bash_pushed') }}",
            'RETURN_VALUE':"{{ ti.xcom_pull(task_ids='bash_push') }}"}, # return_value 찾아오겠다
        bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE",
        do_xcom_push=False # 자동으로 return_value으로 간주되고 Xcom에 저장하는 것을 막음 default=True
        # True로 하면 bash_command의 출력값이 Xcom으로 올라감
    )

    bash_push >> bash_pull