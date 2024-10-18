from airflow import DAG
import pendulum
from airflow.sensors.bash import BashSensor
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='dags_bash_sensor',
    start_date = pendulum.datetime(2024,10,1, tz='Asia/Seoul'),
    schedule="0 6 * * *",
    catchup=False
) as dag:
    
    seneor_task_by_poke = BashSensor(
        task_id = 'seneor_task_by_poke',
        env={'FILE':'/opt/airflow/files/tvCorona19VaccineStatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tvCorona19VaccinestatNew.csv'},
        bash_command=f'''echo $FILE &&
                        if [ -f $FILE ]; then
                            exit 0
                        else 
                            exit 1
                        fi''',
        poke_interval=30,
        timeout=60*2,
        mode='poke',
        soft_fail=False
    )

    seneor_task_by_reschedule = BashSensor(
        task_id = 'seneor_task_by_reschedule',
        env={'FILE':'/opt/airflow/files/tvCorona19VaccineStatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tvCorona19VaccinestatNew.csv'},
        bash_command=f'''echo $FILE &&
                        if [ -f $FILE ]; then
                            exit 0
                        else 
                            exit 1
                        fi''',
        poke_interval=60*2,
        timeout=60*3,
        mode='reschedule',
        soft_fail=True
    )

    bash_task = BashOperator(
        task_id='bash_task',
        env={'FILE':'/opt/airflow/files/tvCorona19VaccineStatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tvCorona19VaccinestatNew.csv'},
        bash_command='echo "건수: `cat $FILE | wc -l`"',
    )

    [seneor_task_by_poke, seneor_task_by_reschedule] >> bash_task