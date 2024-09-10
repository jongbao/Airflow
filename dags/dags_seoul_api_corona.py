from airflow import DAG
import datetime
import pendulum
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="dags_seoul_api_corona",
    # schedule='0 7 * * *',
    schedule=None,
    start_date=pendulum.datetime(2024, 8, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    '''서울시 코로나19 확진자 발생 동향'''
    tb_corona19_count_status = SeoulApiToCsvOperator(
        task_id='tb_corona19_count_status',
        dataset_nm='TbCorona19CountStatus',
        # 태스크 수행하는 주체는 worker 컨테이너 이므로 해당 파일 경로도 worker 컨테이너의 파일 경로임
        # 컨테이너 내려버리면 해당 컨테이너 내부의 데이터는 사라지기 때문에 wsl과 연결해줘야 함
        path='/opt/airflow/files/TbCorona19CountStatus/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='TbCorona19CountStatus.csv'
    )

    '''서울시 코로나19 백신 예방접종 현황'''
    tv_corona19_vaccine_stat_new = SeoulApiToCsvOperator(
        task_id='tv_corona19_vaccine_stat_new',
        dataset_nm='tvCorona19VaccinestatNew',
        path='/opt/airflow/files/tvCorona19VaccinestatNew/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='tvCorona19VaccinestatNew.csv'
    )

    tb_corona19_count_status >> tv_corona19_vaccine_stat_new