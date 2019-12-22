from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.contrib.hooks.ssh_hook import SSHHook 
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

sshHook = SSHHook(ssh_conn_id='infa_ssh')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 9),
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
    'KillerWF2', 
    default_args=default_args, 
    schedule_interval='0 0 1 * *'
    )

latest_only = LatestOnlyOperator(
    task_id='latest_only', 
    dag=dag
    )

infa_wait_WF_30_bash ="""
cd /cygdrive/c/Users/DRECRAP/Desktop
./WFwrapper.sh DRECRAP Wait_WF_30; echo $?
"""

infa_wait_WF_30 = SSHOperator(
    task_id="infa_wait_WF_30",
    ssh_hook=sshHook,
    command=infa_wait_WF_30_bash,
    dag=dag
    )

latest_only >> infa_wait_WF_30