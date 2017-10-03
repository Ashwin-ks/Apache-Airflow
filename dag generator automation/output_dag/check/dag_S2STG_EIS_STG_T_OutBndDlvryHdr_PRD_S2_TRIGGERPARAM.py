#!/usr/bin/env python
import botocore
from airflow import DAG
from datetime import datetime, timedelta
import logging
from airflow.operators import PythonOperator,SnowFlakeOperator,BashOperator
import boto3
from airflow.utils.email import send_email
import time, re
from airflow.models import Variable

from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow import settings
from airflow.models import TaskInstance
from airflow.utils.state import State

start_date = datetime.now()
task_concurrency=15
max_simultaneous_run = 1
schedule_interval=None
support_email_id=""

default_args = {
    'owner':  "EDF",
    'depends_on_past': False,
    'start_date': start_date,
    'email': [support_email_id],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'wait_for_downstream': False,
}
def conditionally_trigger(context, dag_run_obj):
    """This function decides whether or not to Trigger the remote DAG"""

    session = settings.Session()
    success_runs = session.query(TaskInstance).filter(TaskInstance.state == State.SUCCESS).all()
    var_loc_flag = context['params']['dependant_task_name'] in [dag_run.task_id for dag_run in success_runs]
    if (var_loc_flag):
        return dag_run_obj

dag = DAG(
    'S2STG_EIS_STG_T_OutBndDlvryHdr_PRD_S2_TRIGGERPARAM', default_args=default_args, schedule_interval=schedule_interval,
    max_active_runs=max_simultaneous_run,
    concurrency=task_concurrency, start_date=start_date)


task_1_session2_OutBndDlvryHdr_PRD_py = BashOperator(
    task_id='task_1_session2_OutBndDlvryHdr_PRD_py',
    bash_command='python2.7 /home/dev//1_session2_OutBndDlvryHdr_PRD.py',
    dag=dag
)


task_2_copy_to_snowflake_OutBndDlvryHdr_PRD_sql = SnowFlakeOperator(
    task_id='task_2_copy_to_snowflake_OutBndDlvryHdr_PRD_sql',
    sql="s3:/home/dev/compy.py/wf_S2STG_EIS_STG_T_OutBndDlvryHdr_PRD_S2/2_copy_to_snowflake_OutBndDlvryHdr_PRD.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={}
)


task_3_GenerateNewAuditRow_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql = SnowFlakeOperator(
    task_id='task_3_GenerateNewAuditRow_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql',
    sql="s3:/home/dev/compy.py/wf_S2STG_EIS_STG_T_OutBndDlvryHdr_PRD_S2/3_GenerateNewAuditRow_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={}
)


task_4_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql = SnowFlakeOperator(
    task_id='task_4_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql',
    sql="s3:/home/dev/compy.py/wf_S2STG_EIS_STG_T_OutBndDlvryHdr_PRD_S2/4_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={}
)

trigger = TriggerDagRunOperator(task_id='trigger_dagrun',
                                 trigger_dag_id="S2STG_EIS_STG_T_OutBndDlvryHdr_PRD_S3_TRIGGERPARAM",
                                 python_callable=conditionally_trigger,
                                 params={
                                        'dependant_task_name':'task_1_session2_OutBndDlvryHdr_PRD_py' and  'task_3_GenerateNewAuditRow_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql' and  'task_2_copy_to_snowflake_OutBndDlvryHdr_PRD_sql' and  'task_4_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql'},
                                 dag=dag)

task_1_session2_OutBndDlvryHdr_PRD_py.set_downstream(task_2_copy_to_snowflake_OutBndDlvryHdr_PRD_sql)
task_2_copy_to_snowflake_OutBndDlvryHdr_PRD_sql.set_downstream(task_3_GenerateNewAuditRow_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql)
task_3_GenerateNewAuditRow_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql.set_downstream(task_4_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql)
task_4_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql.set_downstream(trigger)