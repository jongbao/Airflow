from airflow import DAG
import datetime
import pendulum
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_base_branch_operator",
    schedule=None,
    start_date=pendulum.datetime(2024, 8, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    # class = 설계도 생성, BaseBranchOperator 부모클래스 상속받음
    class CustomBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):
            import random
            print(context)

            item_list = ['A','B','C']
            selected_item = random.choice(item_list)
            if selected_item == 'A':
                return 'task_a' # 후행 task_id
            elif selected_item in ['B','C']:
                return ['task_b','task_c']

    # 설계도 통해서 실제 수행가능한 객체 만들어야 함
    custom_branch_operator = CustomBranchOperator(task_id='python_branch_task')

    # input에 따라 출력을 다르게 하기 위해
    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    # 기존에는 파이썬 오퍼레이터를 이용해서 객체를 얻었다면, 해당 내용은 함수를 실행시켜서 객체를 얻음
    custom_branch_operator >> [task_a, task_b, task_c]
