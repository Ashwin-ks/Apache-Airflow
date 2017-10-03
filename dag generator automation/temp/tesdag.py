
from airflow.models import DAG
from datetime import datetime
from airflow.operators import BashOperator
from airflow.operators import PythonOperator

dag =DAG(
        dag_id='testdag',
        schedule_interval=None,
        start_date=datetime.now()
)

def check_stat(**kwargs):
    context=kwargs
    print(context['dag_run'].execution_date)
    return context['dag_run'].execution_date

t1=PythonOperator(
    task_id='python_exec_date',
    python_callable=check_stat,
    provide_context=True,
    xcom_push=True,
    dag=dag
    )



t2 = BashOperator(
    task_id='bash_run_id',
    bash_command="task_state_test=$(airflow task_state testdag python_exec_date '{{ ti.xcom_pull(task_ids='python_exec_date') }}')",
    dag=dag)

t1.set_downstream(t2)


