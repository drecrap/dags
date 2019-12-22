from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.contrib.hooks.ssh_hook import SSHHook 
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 10),
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
    'PWF_Air-v1.0.1', 
    default_args=default_args, 
    schedule_interval='0 0 10 * *',
    max_active_runs=1
    )

latest_only = LatestOnlyOperator(
    task_id='latest_only', 
    dag=dag
    )

dummy_wrapper = "/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh PWF_AIR {{var.value.run_id}}"

op1 = SSHOperator(
    task_id="JObA",
    ssh_hook=SSHHook(ssh_conn_id='infa_ssh'),
    command=dummy_wrapper + " A",
    dag=dag)

op2 = SSHOperator(
    task_id="JobB",
    ssh_hook=SSHHook(ssh_conn_id='infa_ssh'),
    command=dummy_wrapper + " B",
    dag=dag)

op3 = SSHOperator(
    task_id="JobC",
    ssh_hook=SSHHook(ssh_conn_id='infa_ssh'),
    command=dummy_wrapper + " C",
    dag=dag)

op4 = SSHOperator(
    task_id="JobD",
    ssh_hook=SSHHook(ssh_conn_id='infa_ssh'),
    command=dummy_wrapper + " D",
    dag=dag)

op5 = SSHOperator(
    task_id="JobE",
    ssh_hook=SSHHook(ssh_conn_id='infa_ssh'),
    command=dummy_wrapper + " E",
    dag=dag)

latest_only >> op1 >> op2 >> op3 >> op4 >> op5