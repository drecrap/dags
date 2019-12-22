from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.contrib.hooks.ssh_hook import SSHHook 
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.subdag_operator import SubDagOperator
from SwfA import sub_dag_a
from SwfB import sub_dag_b
from SwfC import sub_dag_c

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'RWF_Air-v1.0.2',  #update version whenever you change something
    default_args=default_args,
    start_date=datetime(2019, 10, 31),
    schedule_interval='0 0 * * *'
    )

latest_only = LatestOnlyOperator(
    task_id='Nur_Aktueller_Run', 
    dag=dag
    )

op1 = SSHOperator(
    task_id="Job1",
    ssh_hook=SSHHook(ssh_conn_id='infa_ssh'),
    command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} Job1; echo $?",
    do_xcom_push=True,
    dag=dag)

def branch_func_1(**kwargs):
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids='Job1')
    retCode = str(xcom_value)[-4:-3]
    print(retCode)
    if retCode == '0':
        return 'Job1_erfolgreich..Job2'
    else:
        return 'Job1_fehlgeschlagen'

branch_op1 = BranchPythonOperator(
    task_id='Prüfe_Job1-Erfolg',
    provide_context=True,
    python_callable=branch_func_1,
    dag=dag)

op_branch_op1_fail = DummyOperator(
    task_id='Job1_fehlgeschlagen', 
    dag=dag
    )

op2 = SSHOperator(
    task_id="Job1_erfolgreich..Job2",
    ssh_hook=SSHHook(ssh_conn_id='infa_ssh'),
    command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} Job2; echo $?",
    do_xcom_push=True,
    dag=dag)

def branch_func_2(**kwargs):
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids='Job1_erfolgreich..Job2')
    retCode = str(xcom_value)[-4:-3]
    print(retCode)
    if retCode == '0':
        return 'Job2_erfolgreich'
    else:
        return 'Job2_fehlgeschlagen..Job3'

branch_op2 = BranchPythonOperator(
    task_id='Prüfe_Job2-Erfolg',
    provide_context=True,
    python_callable=branch_func_2,
    dag=dag)

op3 = SSHOperator(
    task_id="Job2_fehlgeschlagen..Job3",
    ssh_hook=SSHHook(ssh_conn_id='infa_ssh'),
    command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} Job3; echo $?",
    do_xcom_push=True,
    dag=dag)

def branch_func_3(**kwargs):
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids='Job2_fehlgeschlagen..Job3')
    retCode = str(xcom_value)[-4:-3]
    print(retCode)
    if retCode == '0':
        return 'Prüfe_WF-Erfolg'
    else:
        return 'WF_fehlgeschlagen..Versand_Benachrichtigung'

branch_op3 = BranchPythonOperator(
    task_id='Prüfe_Job3-Erfolg',
    provide_context=True,
    python_callable=branch_func_3,
    dag=dag)

op_split = DummyOperator(
    task_id='Job2_erfolgreich', 
    dag=dag
    )

op3_ = SSHOperator(
    task_id="Job3",
    ssh_hook=SSHHook(ssh_conn_id='infa_ssh'),
    command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} Job3; echo $?",
    do_xcom_push=True,
    dag=dag)

op_swf_a = SubDagOperator(
  subdag=sub_dag_a('RWF_Air-v1.0.2', 'swfA', dag.start_date,
                 dag.schedule_interval),
  task_id='swfA',
  dag=dag,
)

op_swf_b = SubDagOperator(
  subdag=sub_dag_b('RWF_Air-v1.0.2', 'swfB', dag.start_date,
                 dag.schedule_interval),
  task_id='swfB',
  dag=dag,
)

op_swf_c = SubDagOperator(
  subdag=sub_dag_c('RWF_Air-v1.0.2', 'swfC', dag.start_date,
                 dag.schedule_interval),
  task_id='swfC',
  dag=dag,
)

op_merge = DummyOperator(
    task_id='Merge', 
    dag=dag
    )

def branch_func_succ(**kwargs):
    ti = kwargs['ti']
    xcom_value_1 = ti.xcom_pull(task_ids='Job1_erfolgreich..Job2')
    print(xcom_value_1)
    retCode_1 = str(xcom_value_1)[-4:-3]
    xcom_value_2 = ti.xcom_pull(task_ids='Job1')
    print(xcom_value_2)
    xcom_value_42 = ti.xcom_pull(task_ids='branch_task_success', dag_id='RWF_Air-v1.0.2.swfA', key='WfSucc')
    print('RWF_Air-v1.0.2.swfA WfSucc: %s' % xcom_value_42)
    retCode_2 = str(xcom_value_2)[-4:-3]
    print(retCode_1)
    print(retCode_2)
    if retCode_1 == '0' and retCode_2 == '0':
        return 'WF_erfolgreich..Job4'
    else:
        return 'WF_fehlgeschlagen..Versand_Benachrichtigung'

branch_op_succ = BranchPythonOperator(
    task_id='Prüfe_WF-Erfolg',
    trigger_rule= 'one_success',
    provide_context=True,
    python_callable=branch_func_succ,
    dag=dag)

op4 = SSHOperator(
    task_id="WF_erfolgreich..Job4",
    ssh_hook=SSHHook(ssh_conn_id='infa_ssh'),
    command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} Job4",
    dag=dag)

op_fail_mail = DummyOperator(
    task_id='WF_fehlgeschlagen..Versand_Benachrichtigung', 
    trigger_rule= 'one_success',
    dag=dag
    )

op5 = SSHOperator(
    task_id="Job5",
    ssh_hook=SSHHook(ssh_conn_id='infa_ssh'),
    command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} Job5",
    dag=dag)


latest_only >> op1 >> branch_op1 >> [op2, op_branch_op1_fail] 
op_branch_op1_fail >> branch_op_succ
op2 >> branch_op2 >> [op3, op_split]
op3 >> branch_op3 >> [branch_op_succ, op_fail_mail]
op_split >> [op_swf_a, op_swf_b, op3_]
op_swf_a >> op_swf_c
[op_swf_c, op_swf_b, op3_] >> op_merge >> branch_op_succ
branch_op_succ >> [op4, op_fail_mail]
op_fail_mail >> op5
