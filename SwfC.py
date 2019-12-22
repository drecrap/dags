from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook 
from airflow.operators.python_operator import BranchPythonOperator

# Dag is returned by a factory method
def sub_dag_c(parent_dag_name, child_dag_name, start_date, schedule_interval):

    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date)

    def branch_func_1(**kwargs):
        ti = kwargs['ti']
        xcom_value = ti.xcom_pull(task_ids='branch_task_success', dag_id='RWF_Air.swfA', key='WfSucc')
        if xcom_value == '1':
            ti.xcom_push(key='intermediateSucc', value='1')
            return 'JobC1'
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
        dag=dag)

    op1 = SSHOperator(
        task_id="JobC1",
        ssh_hook=SSHHook(ssh_conn_id='infa_ssh'),
        command="/cygdrive/c/Users/DRECRAP/Desktop/DummyWrapper/WFdummyWrapper.sh RWF_AIR {{run_id}} JobC1; echo $?",
        do_xcom_push=True,
        dag=dag
    )
    
    def branch_func_2(**kwargs):
        ti = kwargs['ti']
        #required for checking if SWF A was successful
        xcom_value_1 = ti.xcom_pull(task_ids='branch_task_1', key='intermediateSucc')
        #required for checking if JobC1 was successful
        xcom_value_2 = ti.xcom_pull(task_ids='JobC1')
        retCode = str(xcom_value_2)[-4:-3]

        if xcom_value_1 == '1' and retCode == '0':
            ti.xcom_push(key='WfSucc', value='1')
            return 'ok'
        else:
            ti.xcom_push(key='WfSucc', value='0')
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

    branch_op1 >> [op_branch_intermed_fail, op1]
    [op_branch_intermed_fail, op1] >> branch_op_succ >> [op_fail, op_ok]

    return dag