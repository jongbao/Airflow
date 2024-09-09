from airflow import DAG
import datetime
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_simple_http_operator_budongsan",
    schedule=None,
    start_date=pendulum.datetime(2024, 8, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    '''서울시 부동산 실거래가 정보'''
    tb_budongsan_info = SimpleHttpOperator(
        task_id='tb_budongsan_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{ var.value.apikey_openapi_seoul_go_kr }}/json/xml/tbLnOpendataRtmsV/1/10/', # 코드에 명시하면 보안 문제 있을 수 있기 때문에 varialbes에 등록해서 받아오자
        method='GET',
        headers={'Content-Type': 'application/json',
                 'charset': 'utf-8',
                 'Accept' : '*/*'
                 }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_budongsan_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))

    tb_budongsan_info >> python_2()