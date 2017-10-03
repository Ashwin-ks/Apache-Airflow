#!/usr/bin/env python
import botocore
from airflow import DAG
from datetime import datetime, timedelta,date
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
from airflow.models import DagRun
from sqlalchemy import func

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
    if context['params']['condition_param']:
        dag_run_obj.payload = {'message': context['params']['message']}
        return dag_run_obj

dag = DAG(
    'dag_one_to_many', default_args=default_args, schedule_interval=schedule_interval,
    max_active_runs=max_simultaneous_run,
    concurrency=task_concurrency, start_date=start_date)



task_1_EISBatchGroup_Ins_EIS_BATCHGRPLOG_sql = SnowFlakeOperator(
    task_id='task_1_EISBatchGroup_Ins_EIS_BATCHGRPLOG_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/dags/1_EISBatchGroup_Ins_EIS_BATCHGRPLOG.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCHDR_D', 'TABLE_NAME_XI': 'LIKP_D_XI', 'PARAMTGTTABLE': 'LIKP_D_XI', 'SCHEMA_STG': 'EIS_STG_T', 'SRCSYS': "'R3_PRD'", 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/DEV', 'DATASRC': "'2LIS_12_VCHDR'", 'PSANAME': "'2LIS_12_VCHDR%'", 'BATCH_GROUP': 'USA', 'ORACLE_EXPORT_SQL': 'oracle_output1.sql', 'TABLE_NAME': ' DEV', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/DEV'}
)


task_2_EISBatchGroup_Upd_EIS_REQUEST_LOG_sql = SnowFlakeOperator(
    task_id='task_2_EISBatchGroup_Upd_EIS_REQUEST_LOG_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/dags/2_EISBatchGroup_Upd_EIS_REQUEST_LOG.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCHDR_D', 'TABLE_NAME_XI': 'LIKP_D_XI', 'PARAMTGTTABLE': 'LIKP_D_XI', 'SCHEMA_STG': 'EIS_STG_T', 'SRCSYS': "'R3_PRD'", 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/DEV', 'DATASRC': "'2LIS_12_VCHDR'", 'PSANAME': "'2LIS_12_VCHDR%'", 'BATCH_GROUP': 'USA', 'ORACLE_EXPORT_SQL': 'oracle_output1.sql', 'TABLE_NAME': ' DEV', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/DEV'}
)


task_3_EISBatchGroup_Upd_ETLTableAudit_sql = SnowFlakeOperator(
    task_id='task_3_EISBatchGroup_Upd_ETLTableAudit_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/dags/3_EISBatchGroup_Upd_ETLTableAudit.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCHDR_D', 'TABLE_NAME_XI': 'LIKP_D_XI', 'PARAMTGTTABLE': 'LIKP_D_XI', 'SCHEMA_STG': 'EIS_STG_T', 'SRCSYS': "'R3_PRD'", 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/DEV', 'DATASRC': "'2LIS_12_VCHDR'", 'PSANAME': "'2LIS_12_VCHDR%'", 'BATCH_GROUP': 'USA', 'ORACLE_EXPORT_SQL': 'oracle_output1.sql', 'TABLE_NAME': ' DEV', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/DEV'}
)

trigger1=TriggerDagRunOperator(task_id='branch1',
                                trigger_dag_id='dag_schedule_test1',
                                python_callable=conditionally_trigger,
                                params={'condition_param':True,
                                        'message':'Hello1'},
                                dag=dag)

trigger2=TriggerDagRunOperator(task_id='branch2',
                                trigger_dag_id='dag_schedule_test2',
                                python_callable=conditionally_trigger,
                                params={'condition_param':True,
                                        'message':'Hello2'},
                                dag=dag)


task_1_EISBatchGroup_Ins_EIS_BATCHGRPLOG_sql.set_downstream(task_2_EISBatchGroup_Upd_EIS_REQUEST_LOG_sql)
task_2_EISBatchGroup_Upd_EIS_REQUEST_LOG_sql.set_downstream(task_3_EISBatchGroup_Upd_ETLTableAudit_sql)
task_3_EISBatchGroup_Upd_ETLTableAudit_sql.set_downstream(trigger1)
task_3_EISBatchGroup_Upd_ETLTableAudit_sql.set_downstream(trigger2)
