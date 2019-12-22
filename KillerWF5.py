from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.contrib.hooks.ssh_hook import SSHHook 
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.python_operator import BranchPythonOperator

sshHook = SSHHook(ssh_conn_id='infa_ssh')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 31),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'KillerWF5', 
    default_args=default_args, 
    schedule_interval='0 0 5 * *'
    )

infa_Fail_WF_bash ="""
cd /cygdrive/c/Users/DRECRAP/Desktop
./WFwrapper.sh DRECRAP Fail_WF; echo $?
"""

latest_only = LatestOnlyOperator(
    task_id='latest_only', 
    dag=dag
    )

start_op = SSHOperator(
    task_id="infa_Fail_WF",
    ssh_hook=sshHook,
    command=infa_Fail_WF_bash,
    do_xcom_push=True,
    dag=dag)

def branch_func(**kwargs):
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids='infa_Fail_WF')
    retCode = str(xcom_value)[-4:-3]
    print(retCode)
    if retCode == '0':
        return 'job_sucess'
    else:
        return 'job_fail'

branch_op = BranchPythonOperator(
    task_id='eval_job_success',
    provide_context=True,
    python_callable=branch_func,
    dag=dag)

succ_op = BashOperator(
    task_id='job_sucess',
    bash_command='echo "job was successful"',
    dag=dag
)

fail_op = BashOperator(
    task_id='job_fail',
    bash_command='echo "job failed"',
    dag=dag
)

join_op = BashOperator(
    task_id='echo_after_join', 
    bash_command='echo "Wird unabhängig vom INFA WF-Erfolg ausgegeben"',
    trigger_rule= 'one_success',
    dag=dag
)

latest_only >> start_op >> branch_op >> [succ_op, fail_op] >> join_op
