
from datetime import datetime
from airflow.models import DAG
from airflow.hooks.base_hook import BaseHook
from custom_operators.dag2.my_operator import ExampleOperator

connection = BaseHook.get_connection("postgres_conn")

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 29),
}

dag = DAG('dag2', default_args=default_args, schedule_interval='0 * * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["Test2"])

task1 = ExampleOperator(
    task_id='task1',
    postgre_conn=connection,
    currency='BTC',
    value='20200',
    dag=dag)

task2 = ExampleOperator(
    task_id='task2',
    postgre_conn=connection,
    currency='ETH',
    value='1200',
    dag=dag)

task1 >> task2