from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook 
from airflow.operators.python_operator import BranchPythonOperator

# Dag is returned by a factory method
def sub_dag_b(parent_dag_name, child_dag_name, start_date, schedule_interval):

    sshHook = SSHHook(ssh_conn_id='infa_ssh')

    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    op_split_1 = DummyOperator(
        task_id='split_1', 
        dag=dag
    )

    op1 = SSHOperator(
        task_id="JobB1",
        ssh_hook=sshHook,
        command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} JobB1; echo $?",
        do_xcom_push=True,
        dag=dag
    )

    op2 = SSHOperator(
        task_id="JobB2",
        ssh_hook=sshHook,
        command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} JobB2; echo $?",
        do_xcom_push=True,
        dag=dag
    )

    op_join_1 = DummyOperator(
        task_id='join_1', 
        dag=dag
    )

    def branch_func_1(**kwargs):
        ti = kwargs['ti']
        xcom_value_1 = ti.xcom_pull(task_ids='JobB1')
        xcom_value_2 = ti.xcom_pull(task_ids='JobB2')
        retCode_1 = str(xcom_value_1)[-4:-3]
        retCode_2 = str(xcom_value_2)[-4:-3]
        if retCode_1 == '0' and retCode_2 == '0':
            ti.xcom_push(key='intermediateSucc', value='1')
            return 'split_2'
        else:
            ti.xcom_push(key='intermediateSucc', value='0')
            return 'goto_branch_task_succ'

    branch_op1 = BranchPythonOperator(
        task_id='branch_task_1',
        provide_context=True,
        python_callable=branch_func_1,
        dag=dag)

    op_branch_intermed_fail = DummyOperator(
    task_id='goto_branch_task_succ', 
    dag=dag
    )

    op_split_2 = DummyOperator(
        task_id='split_2', 
        dag=dag
    )

    op3 = SSHOperator(
        task_id="JobB3",
        ssh_hook=sshHook,
        command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} JobB3; echo $?",
        do_xcom_push=True,
        dag=dag
    )

    op4 = SSHOperator(
        task_id="JobB4",
        ssh_hook=sshHook,
        command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} JobB4; echo $?",
        do_xcom_push=True,
        dag=dag
    )

    op_join_2 = DummyOperator(
        task_id='join_2', 
        dag=dag
    )
    
    def branch_func_2(**kwargs):
        ti = kwargs['ti']
        #required for checking if JobB1 and B2 were successful
        xcom_value_1 = ti.xcom_pull(task_ids='branch_task_1', key='intermediateSucc')
        #required for checking if JobB3 and B4 were successful
        xcom_value_2 = ti.xcom_pull(task_ids='JobB3')
        xcom_value_3 = ti.xcom_pull(task_ids='JobB4')
        retCode_1 = str(xcom_value_1)[-4:-3]
        retCode_2 = str(xcom_value_2)[-4:-3]

        if xcom_value_1 == '1' and retCode_1 == '0' and retCode_2 == '0':
            ti.xcom_push(key='WfSucc', value='1')
            return 'ok'
        else:
            ti.xcom_push(key='WfSucc', value='1')
            return 'fail'

    branch_op_succ = BranchPythonOperator(
        task_id='branch_task_success',
        provide_context=True,
        trigger_rule= 'one_success',
        python_callable=branch_func_2,
        dag=dag)

    op_ok = DummyOperator(
        task_id='ok', 
        dag=dag
    )

    op_fail = DummyOperator(
        task_id='fail', 
        dag=dag
    )

    op_split_1 >> [op1, op2] >> op_join_1 >> branch_op1 >> [op_split_2, op_branch_intermed_fail]
    op_split_2 >> [op3, op4] >> op_join_2  
    [op_branch_intermed_fail, op_join_2] >> branch_op_succ >> [op_ok, op_fail]

    return dag