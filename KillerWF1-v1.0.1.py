from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'KillerWF1-v1.0.1', 
    default_args=default_args, 
    schedule_interval='0 0 1 * *'
    )

latest_only = LatestOnlyOperator(
    task_id='latest_only', 
    dag=dag
    )

templated_command = """
    echo hi
"""

t1 = BashOperator(
    task_id='templated',
    bash_command=templated_command,
    dag=dag)

latest_only >> t1