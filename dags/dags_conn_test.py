from airflow import DAG
import datetime
import pendulum
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_conn_test",
    schedule=None,
    start_date=pendulum.datetime(2024,8,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    t1 = EmptyOperator(
        task_id="t1"
    )

    t2 = EmptyOperator(
        task_id="t2"
    )

    t3 = EmptyOperator(
        task_id="t3"
    )

    t4 = EmptyOperator(
        task_id="t4"
    )

    t5 = EmptyOperator(
        task_id="t5"
    )

    t6 = EmptyOperator(
        task_id="t6"
    )

    t7 = EmptyOperator(
        task_id="t7"
    )

    t8 = EmptyOperator(
        task_id="t8"
    )

t1 >> [t2, t3] >> t4 << t5 # 같은 레벨에서 수행되는 task는 리스트로 묶을 수 있음
[t4, t7] >> t6 >> t8


# t1 >> [t2,t3]
# t3 << t4

# t1.set_downstream(t2, t3)
# t3.set_upstream(t4)