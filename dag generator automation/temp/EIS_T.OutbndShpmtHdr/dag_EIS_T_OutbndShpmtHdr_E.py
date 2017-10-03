#!/usr/bin/env python
import botocore
from airflow import DAG
from datetime import datetime, timedelta
import logging
from airflow.operators import PythonOperator, DummyOperator, SnowFlakeOperator
from airflow.hooks import SnowFlakeHook
import boto3
from airflow.utils.email import send_email
import time, re
from airflow.models import Variable
import airflow
start_date = datetime(year=2017, month=3, day=28, hour=21, minute=25)
task_concurrency=15
max_simultaneous_run = 1
schedule_interval= '0 24 * * *'
support_email_id=""

default_args = {
    'owner': "EDF",
    'depends_on_past': False,
    'start_date': start_date,
    'email': [support_email_id],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'wait_for_downstream': False,
}

dag = DAG(
    'EIS_T_OutbndShpmtHdr_E', default_args=default_args, schedule_interval=schedule_interval,
    max_active_runs=max_simultaneous_run,
    concurrency=task_concurrency, start_date=start_date)


success_audit_sql="""UPDATE  EIS_T.A_ETL_TABLE_AUDIT
set NBR_OF_SOURCE_ROWS=(Select count(*) from EIS_STG_T.OutbndShpmtHdr_E where ROWLASTUPDTFK = (select ETL_TABLE_AUDIT_PK from EIS_T.A_ETL_TABLE_AUDIT
where table_name=upper('OutbndShpmtHdr[EMEA]') and TBL_ETL_STATUS='In Progress'))
where table_name=upper('OutbndShpmtHdr[EMEA]') and TBL_ETL_STATUS='In Progress';

Update EIS_T.A_ETL_TABLE_AUDIT
set TBL_ETL_STATUS=  'Completed', TBL_ETL_END_TMST= current_date()
Where table_name=upper('OutbndShpmtHdr[EMEA]')  and TBL_ETL_STATUS='In Progress';"""

failure_audit_sql="""Update EIS_T.A_ETL_TABLE_AUDIT  set TBL_ETL_STATUS='Failed', TBL_ETL_END_TMST= current_date()
Where table_name=upper('OutbndShpmtHdr[EMEA]') and TBL_ETL_STATUS='In Progress';"""

def success_callback():
    snowflake_auditor = SnowFlakeHook(conn_id="snowflake")
    success_audit_sql_list= success_audit_sql.split(";")
    for sql in success_audit_sql_list :
        snowflake_auditor.run(autocommit=True, sql=sql)

def failure_callback(context):
    snowflake_auditor = SnowFlakeHook(conn_id="snowflake")
    failure_audit_sql_list= failure_audit_sql.split(";")
    for sql in failure_audit_sql_list :
        snowflake_auditor.run(autocommit=True, sql=sql)


# This is the end node
finished_all = PythonOperator(
    task_id='finished_all',
    python_callable=success_callback,
    trigger_rule="one_success",
    dag=dag)
#This tasks runs s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/s2stg_workflows/OutbndShpmtHdr/wf_EIS_T.OutbndShpmtHdr/3_c.sql
task_3_c_sql = SnowFlakeOperator(
    task_id='task_task_3_c_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/s2stg_workflows/OutbndShpmtHdr/wf_EIS_T.OutbndShpmtHdr/3_c.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=failure_callback,
    parameters={'SCHEDULE_INTERVAL': "'0 24 * * *'", 'START_DATE': 'datetime(year=2017, month=3, day=28, hour=21, minute=25)', 'TGTDB': 'EIS_T', 'SOURCE_SYSTEM': '1', 'STGDB': 'EIS_STG_T', 'TABLE_NAME_IN_AUDIT': 'OutbndShpmtHdr[EMEA]', 'VIEW_NAME': 'VTTK_D_xi_V_PRD', 'RG': 'E', 'BATCHGRP': 'O_EMEA', 'VWDB': 'EIS', 'SRC_REGION_TABLE': 'ADDR_PRD', 'TGT_REGION_TABLE': 'OutbndShpmtHdr_E', 'DBNAME': 'EIS_STG_T', 'ETLDB': 'EIS_ETL'}
)

#This tasks runs s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/s2stg_workflows/OutbndShpmtHdr/wf_EIS_T.OutbndShpmtHdr/1_a.sql
task_1_a_sql = SnowFlakeOperator(
    task_id='task_task_1_a_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/s2stg_workflows/OutbndShpmtHdr/wf_EIS_T.OutbndShpmtHdr/1_a.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=failure_callback,
    parameters={'SCHEDULE_INTERVAL': "'0 24 * * *'", 'START_DATE': 'datetime(year=2017, month=3, day=28, hour=21, minute=25)', 'TGTDB': 'EIS_T', 'SOURCE_SYSTEM': '1', 'STGDB': 'EIS_STG_T', 'TABLE_NAME_IN_AUDIT': 'OutbndShpmtHdr[EMEA]', 'VIEW_NAME': 'VTTK_D_xi_V_PRD', 'RG': 'E', 'BATCHGRP': 'O_EMEA', 'VWDB': 'EIS', 'SRC_REGION_TABLE': 'ADDR_PRD', 'TGT_REGION_TABLE': 'OutbndShpmtHdr_E', 'DBNAME': 'EIS_STG_T', 'ETLDB': 'EIS_ETL'}
)

#This tasks runs s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/s2stg_workflows/OutbndShpmtHdr/wf_EIS_T.OutbndShpmtHdr/2_b.sql
task_2_b_sql = SnowFlakeOperator(
    task_id='task_task_2_b_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/s2stg_workflows/OutbndShpmtHdr/wf_EIS_T.OutbndShpmtHdr/2_b.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=failure_callback,
    parameters={'SCHEDULE_INTERVAL': "'0 24 * * *'", 'START_DATE': 'datetime(year=2017, month=3, day=28, hour=21, minute=25)', 'TGTDB': 'EIS_T', 'SOURCE_SYSTEM': '1', 'STGDB': 'EIS_STG_T', 'TABLE_NAME_IN_AUDIT': 'OutbndShpmtHdr[EMEA]', 'VIEW_NAME': 'VTTK_D_xi_V_PRD', 'RG': 'E', 'BATCHGRP': 'O_EMEA', 'VWDB': 'EIS', 'SRC_REGION_TABLE': 'ADDR_PRD', 'TGT_REGION_TABLE': 'OutbndShpmtHdr_E', 'DBNAME': 'EIS_STG_T', 'ETLDB': 'EIS_ETL'}
)

task_1_a_sql.set_downstream(task_2_b_sql)
task_2_b_sql.set_downstream(task_3_c_sql)
task_3_c_sql.set_downstream(finished_all)