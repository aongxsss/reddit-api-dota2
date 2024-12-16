from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime


default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'dummy_dag',
    default_args=default_args,
    description='A simple dummy DAG',
    schedule_interval=None,  
    catchup=False,
) as dag:

   
    start = DummyOperator(
        task_id='start'
    )

    end = DummyOperator(
        task_id='end'
    )

   
    start >> end
