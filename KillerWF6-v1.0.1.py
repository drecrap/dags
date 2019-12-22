from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook 
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 6),
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
    'KillerWF6-v1.0.1', 
    default_args=default_args, 
    schedule_interval='0 0 6 * *'
    )

infa_wait_WF_10_bash ="""
cd /cygdrive/c/Users/DRECRAP/Desktop
./WFwrapper.sh DRECRAP Wait_WF_10; echo $?
"""

infa_wait_WF_30_bash ="""
cd /cygdrive/c/Users/DRECRAP/Desktop
./WFwrapper.sh DRECRAP Wait_WF_30; echo $?
"""

latest_only = LatestOnlyOperator(
    task_id='latest_only', 
    dag=dag
    )

op1 = BashOperator(
    task_id='echo_start',
    bash_command='echo start',
    dag=dag,
)

op2 = SSHOperator(
    task_id='echo_wait_WF_10',
    ssh_hook=SSHHook(ssh_conn_id='infa_ssh'),
    command=infa_wait_WF_10_bash,
    do_xcom_push=True,
    dag=dag)

op3 = SSHOperator(
    task_id='echo_wait_WF_30',
    ssh_hook=SSHHook(ssh_conn_id='infa_ssh'),
    command=infa_wait_WF_30_bash,
    do_xcom_push=True,
    dag=dag)

op4 = BashOperator(
    task_id='echo_done',
    bash_command='echo "Wird ausgegeben sobald alle Vorgänger abgearbeitet worden sind"',
    dag=dag,
)

latest_only >> op1 >> [op2, op3] >> op4