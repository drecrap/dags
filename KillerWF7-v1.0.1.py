from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.contrib.hooks.ssh_hook import SSHHook 
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.python_operator import BranchPythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 7),
    'email': ['drechsler@integration-factory.de'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'KillerWF7-v1.0.1', 
    default_args=default_args, 
    schedule_interval='0 0 7 * *'
    )

latest_only = LatestOnlyOperator(
    task_id='latest_only', 
    dag=dag
    )

fail_op = BashOperator(
    task_id='job_fail',
    bash_command='exit 1',
    dag=dag
)

end_op = DummyOperator(
    task_id='end_task', 
    trigger_rule= 'none_skipped',
    dag=dag
)

latest_only >> fail_op >> end_op
