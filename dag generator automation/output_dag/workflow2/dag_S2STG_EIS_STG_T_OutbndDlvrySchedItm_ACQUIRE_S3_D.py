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
    'S2STG_EIS_STG_T_OutbndDlvrySchedItm_ACQUIRE_S3_D', default_args=default_args, schedule_interval=schedule_interval,
    max_active_runs=max_simultaneous_run,
    concurrency=task_concurrency, start_date=start_date)



task_1_EISBatchGroup_Ins_EIS_BATCHGRPLOG_OutbndDlvrySchedItm_sql = SnowFlakeOperator(
    task_id='task_1_EISBatchGroup_Ins_EIS_BATCHGRPLOG_OutbndDlvrySchedItm_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/S2STG_workflows/OutbndDlvrySchedItm/wf_S2STG_EIS_STG_T_OutbndDlvrySchedItm_ACQUIRE_S3/1_EISBatchGroup_Ins_EIS_BATCHGRPLOG_OutbndDlvrySchedItm.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCITM_D', 'TABLE_NAME_XI': 'LIPS_OUTB_D_xi', 'PARAMTGTTABLE': 'LIPS_OUTB_D_xi', 'SCHEMA_STG': 'EIS_STG_T', 'SRCSYS': "'R3_PRD'", 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/OutbndDlvrySchedItm_PRD', 'DATASRC': "'2LIS_12_VCITM'", 'PSANAME': "'2LIS_12_VCITM%'", 'BATCH_GROUP': 'USA', 'ORACLE_EXPORT_SQL': 'oracle_output1.sql', 'TABLE_NAME': 'OutbndDlvrySchedItm_PRD', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/OutbndDlvrySchedItm_PRD'}
)


task_2_EISBatchGroup_Upd_EIS_REQUEST_LOG_OutbndDlvrySchedItm_sql = SnowFlakeOperator(
    task_id='task_2_EISBatchGroup_Upd_EIS_REQUEST_LOG_OutbndDlvrySchedItm_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/S2STG_workflows/OutbndDlvrySchedItm/wf_S2STG_EIS_STG_T_OutbndDlvrySchedItm_ACQUIRE_S3/2_EISBatchGroup_Upd_EIS_REQUEST_LOG_OutbndDlvrySchedItm.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCITM_D', 'TABLE_NAME_XI': 'LIPS_OUTB_D_xi', 'PARAMTGTTABLE': 'LIPS_OUTB_D_xi', 'SCHEMA_STG': 'EIS_STG_T', 'SRCSYS': "'R3_PRD'", 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/OutbndDlvrySchedItm_PRD', 'DATASRC': "'2LIS_12_VCITM'", 'PSANAME': "'2LIS_12_VCITM%'", 'BATCH_GROUP': 'USA', 'ORACLE_EXPORT_SQL': 'oracle_output1.sql', 'TABLE_NAME': 'OutbndDlvrySchedItm_PRD', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/OutbndDlvrySchedItm_PRD'}
)


task_3_EISBatchGroup_Upd_ETLTableAudit_OutbndDlvrySchedItm_sql = SnowFlakeOperator(
    task_id='task_3_EISBatchGroup_Upd_ETLTableAudit_OutbndDlvrySchedItm_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/S2STG_workflows/OutbndDlvrySchedItm/wf_S2STG_EIS_STG_T_OutbndDlvrySchedItm_ACQUIRE_S3/3_EISBatchGroup_Upd_ETLTableAudit_OutbndDlvrySchedItm.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCITM_D', 'TABLE_NAME_XI': 'LIPS_OUTB_D_xi', 'PARAMTGTTABLE': 'LIPS_OUTB_D_xi', 'SCHEMA_STG': 'EIS_STG_T', 'SRCSYS': "'R3_PRD'", 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/OutbndDlvrySchedItm_PRD', 'DATASRC': "'2LIS_12_VCITM'", 'PSANAME': "'2LIS_12_VCITM%'", 'BATCH_GROUP': 'USA', 'ORACLE_EXPORT_SQL': 'oracle_output1.sql', 'TABLE_NAME': 'OutbndDlvrySchedItm_PRD', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/OutbndDlvrySchedItm_PRD'}
)

task_1_EISBatchGroup_Ins_EIS_BATCHGRPLOG_OutbndDlvrySchedItm_sql.set_downstream(task_2_EISBatchGroup_Upd_EIS_REQUEST_LOG_OutbndDlvrySchedItm_sql)
task_2_EISBatchGroup_Upd_EIS_REQUEST_LOG_OutbndDlvrySchedItm_sql.set_downstream(task_3_EISBatchGroup_Upd_ETLTableAudit_OutbndDlvrySchedItm_sql)