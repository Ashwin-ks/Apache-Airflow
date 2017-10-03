#!/usr/bin/env python
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import BashOperator
from airflow.utils.email import send_email
import time, re

start_date =  datetime(2017,06,29)
task_concurrency=15
max_simultaneous_run = 1
schedule_interval='30 4 * * *'
support_email_id=""

default_args = {
    'owner': "EDF",
    'depends_on_past': False,
    'start_date': start_date,
    'email': ['Ashwin.Kandera@nike.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'wait_for_downstream': False,
}

dag = DAG(
    'dag_schedule_test1', default_args=default_args, schedule_interval=schedule_interval,
    max_active_runs=max_simultaneous_run,
    concurrency=task_concurrency, start_date=start_date)

task_1 = BashOperator(
    task_id='task_1_test1',
    bash_command='echo "Test print1"',
    dag=dag
    )

task_2 = BashOperator(
    task_id='task_2_test1',
    bash_command='echo "Test print1"',
    dag=dag
    )

task_1.set_downstream(task_2)



