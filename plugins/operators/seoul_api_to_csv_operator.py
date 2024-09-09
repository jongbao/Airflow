from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_coon_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{ var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt = base_dt

    '''
    서울시 공공 API 호출 시 endpoint 구조
    url:8080/apikey/type(json,xml..)/dataset_nm/start/end
    '''

    # 오퍼레이터 호출할 때 실행할 코드
    def execute(self, context):
        import os

        # airflow UI에서 만들었던 connection 정보를 꺼내올 수 있음
        connection = BaseHook.get_connection(self.http_coon_id)
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000
        while True:
            self.log.info(f'시작:{start_row}')
            self.log.info(f'끝:{end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df])
            if len(total_row_df) < 1000:
                break
            else:
                start_row = end_row + 1
                end_row += 1000

        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

        # get_connection으로 connection 정보 받아올 수 있음

    # API 결과값을 데이터프레임으로 말아서 return하는 함수
    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json

        headers={'Content-Type': 'application/json',
                 'charset': 'utf-8',
                 'Accept' : '*/*'
                 }
        
        request_url = f'{base_url}/{start_row}/{end_row}/'
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        # API 요청
        response = requests.get(request_url, headers)
        contents = json.loads(response.text)

        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        row_df = pd.DataFrame(row_data)

        return row_df        