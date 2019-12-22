from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.contrib.hooks.ssh_hook import SSHHook 
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 11),
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
    'PWF2_Air', 
    default_args=default_args, 
    schedule_interval='0 0 10 * *',
    max_active_runs=1
    )

latest_only = LatestOnlyOperator(
    task_id='latest_only', 
    dag=dag
    )

op1 = BashOperator(
    task_id='JObA',
    bash_command='cd /home/DRECRAP/airflow/perfTests/ && touch A.log',
    dag=dag)

op2 = BashOperator(
    task_id='JObB',
    bash_command='cd /home/DRECRAP/airflow/perfTests/ && touch B.log',
    dag=dag)

op3 = BashOperator(
    task_id='JObC',
    bash_command='cd /home/DRECRAP/airflow/perfTests/ && touch C.log',
    dag=dag)

op4 = BashOperator(
    task_id='JObD',
    bash_command='cd /home/DRECRAP/airflow/perfTests/ && touch D.log',
    dag=dag)

op5 = BashOperator(
    task_id='JObE',
    bash_command='cd /home/DRECRAP/airflow/perfTests/ && touch E.log',
    dag=dag)

templated_command = """
cd /home/DRECRAP/airflow/perfTests/ && 
for f in *
do
  fulldate="$(stat -c %y $f)"
  shortdate="${fulldate:0:23}"
  echo -n "$f, " 
  date -d "$shortdate" +%s%3N
done
"""

op6 = BashOperator(
    task_id='getDates',
    bash_command=templated_command,
    dag=dag)

latest_only >> op1 >> op2 >> op3 >> op4 >> op5 >> op6