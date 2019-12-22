from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook 
from airflow.operators.python_operator import BranchPythonOperator

# Dag is returned by a factory method
def sub_dag_a(parent_dag_name, child_dag_name, start_date, schedule_interval):

    sshHook = SSHHook(ssh_conn_id='infa_ssh')

    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    op_split = DummyOperator(
        task_id='split', 
        dag=dag
    )

    op1 = SSHOperator(
        task_id="JobA1",
        ssh_hook=sshHook,
        command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} JobA1; echo $?",
        do_xcom_push=True,
        dag=dag
    )

    op2 = SSHOperator(
        task_id="JobA2",
        ssh_hook=sshHook,
        command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} JobA2; echo $?",
        do_xcom_push=True,
        dag=dag
    )

    op3 = SSHOperator(
        task_id="JobA3",
        ssh_hook=sshHook,
        command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} JobA3; echo $?",
        do_xcom_push=True,
        dag=dag
    )

    op4 = SSHOperator(
        task_id="JobA4",
        ssh_hook=sshHook,
        command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} JobA4; echo $?",
        do_xcom_push=True,
        dag=dag
    )

    op5 = SSHOperator(
        task_id="JobA5",
        ssh_hook=sshHook,
        command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} JobA5; echo $?",
        do_xcom_push=True,
        dag=dag
    )

    op_join = DummyOperator(
        task_id='join', 
        dag=dag
    )

    def branch_func(**kwargs):
        ti = kwargs['ti']
        if True:
            ti.xcom_push(key='WfSucc', value='1')
            return 'ok'
        else:
            ti.xcom_push(key='WfSucc', value='0')
            return 'fail'

    branch_op_succ = BranchPythonOperator(
        task_id='branch_task_success',
        provide_context=True,
        python_callable=branch_func,
        dag=dag)

    op_ok = DummyOperator(
        task_id='ok', 
        dag=dag
    )

    op_fail = DummyOperator(
        task_id='fail', 
        dag=dag
    )

    op_split >> [op1,op2,op3,op4,op5] >> op_join >> branch_op_succ >> [op_ok, op_fail]

    return dag