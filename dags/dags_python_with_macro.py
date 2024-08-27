from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task 
import pendulum
import datetime

with DAG(
    dag_id='dags_python_with_macro',
    schedule='10 0 * * *',
    start_date=pendulum.datetime(2024,8,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    # 만약 배치일이 3월 15일이라면
    # start_date는 2월 1일(전월 1일), end_date는 2월 28일(전월 말일)
    # 1. macro 변수
    @task(task_id='task_using_macros',
          # 해당 변수가 그대로 kwargs의 key 값이 되고 {} 안의 전체 값이 value로 들어감
          templates_dict={'start_date':'{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, days=1)) | ds}}',
                          'end_date':'{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds}}'})
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs.get('templates_dict') or {}
        if templates_dict:
            start_date = templates_dict.get('start_date') or 'start_date없음'
            end_date = templates_dict.get('end_date') or 'end_date없음'
            print(start_date)
            print(end_date)

    # 2. python 라이브러리 사용
    @task(task_id='task_direct_calc')
    def get_datetime_calc(**kwargs):
        # 스케줄러 부하 경감을 위해 
        # 스케줄러는 주기적으로 우리가 만든 dag에 오류가 있는지 없는지 파싱 하는데 그러면 dag이 실행되지 않아도 주기적으로 검사함
        # 하지만 def 안에 import 하면 검사하지 않아서 부하가 줄어듦
        # 오퍼레이터 안에서만 쓰는 라이브러리는 오퍼레이터 내부에만 작성해라
        from dateutil.relativedelta import relativedelta

        data_interval_end = kwargs['data_interval_end']
        prev_month_day_first = data_interval_end.in_timezone("Asia/Seoul") + relativedelta(months=-1, day=1)
        prev_month_day_last = data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + relativedelta(days=-1)
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))

    get_datetime_macro() >> get_datetime_calc()