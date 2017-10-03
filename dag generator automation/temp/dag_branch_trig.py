from airflow import settings
from airflow.models import TaskInstance
from airflow.models import DagRun
from airflow.utils.state import State

session = settings.Session()

from airflow import DAG
from datetime import datetime, timedelta
import logging
from airflow.operators import PythonOperator,SnowFlakeOperator,BashOperator,BranchPythonOperator
import boto3
from airflow.utils.email import send_email
import time, re
from airflow.models import Variable

from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow import settings
from airflow.models import TaskInstance
from airflow.utils.state import State

start_date = datetime(2017,06,30)
task_concurrency=15
max_simultaneous_run = 1
schedule_interval="0 20 * * *"
support_email_id="Ashwin.Kandera@nike.com"

default_args = {
    'owner':  "EDF",
    'depends_on_past': False,
    'start_date': start_date,
    'email': [support_email_id],
    'email_on_failure': True,
    'email_on_success':True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'wait_for_downstream': False,
}

def conditionally_trigger(context, dag_run_obj):
    """This function decides whether or not to Trigger the remote DAG"""
    c_p =context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
        dag_run_obj.payload = {'message': context['params']['message']}
        print(dag_run_obj.payload)
        return dag_run_obj

def checkRunCount(**context):
    print(context)
    try:
	dag = context.get('dag',None)
	execution_date = context.get('execution_date', None)
        dagrun = dag.get_dagrun(execution_date='06-30T20:10:25')
        if dagrun:
            return "trigger1"
        else:
 	    return "trigger2"
    except:
	return "trigger2"
    
dag = DAG(
    'dag_trig', default_args=default_args, schedule_interval=schedule_interval,
    max_active_runs=max_simultaneous_run,
    concurrency=task_concurrency, start_date=start_date)

task_1 = BashOperator(
    task_id='task_1_check1',
    bash_command='echo "Test print1"',
    dag=dag
    )

task_2 = BashOperator(
    task_id='task_check2',
    bash_command='echo "Test print1"',
    dag=dag
    )

trigger1=TriggerDagRunOperator(task_id='trigger1',
				trigger_dag_id='dag_schedule_test1',
				python_callable=conditionally_trigger,
				params={'condition_param':True,
					'message':'Hello1'},
				dag=dag)

trigger2=TriggerDagRunOperator(task_id='trigger2',
				trigger_dag_id='dag_schedule_test2',
				python_callable=conditionally_trigger,
				params={'condition_param':True,
					'message':'Hello2'},
                                dag=dag)
task_branch=BranchPythonOperator(task_id='branch',
				python_callable=checkRunCount,
				provide_context=True,
				dag=dag)


task_1.set_downstream(task_2)
task_branch.set_downstream(trigger1)
task_branch.set_downstream(trigger2)
task_2.set_downstream(task_branch)


