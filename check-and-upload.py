from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='upload_when_data_ready',
    default_args=args,
    schedule_interval="@daily",
    tags=['precheck-upload']
)


def branch_func(**kwargs):
    ti = kwargs['ti']
    data_is_ready = ti.xcom_pull(task_ids='start_task')
    if data_is_ready.upper() == "TRUE":
        return 'upload_task'
    else:
        return 'not_yet_ready'

start_op = BashOperator(
    task_id='start_task',
    bash_command="echo false",
    xcom_push=True,
    dag=dag)

branch_op = BranchPythonOperator(
    task_id='upload_decision_op',
    provide_context=True,
    python_callable=branch_func,
    dag=dag)

upload_op = BashOperator(task_id='upload_task', bash_command="echo 'i am running the upload'", dag=dag)
not_ready_op = BashOperator(task_id='not_yet_ready', bash_command="echo 'DATA NOT READY'", dag=dag)

start_op >> branch_op >> [upload_op, not_ready_op]