#!/usr/bin/env python
import botocore
from airflow import DAG
from datetime import datetime, timedelta, date
import logging
from airflow.operators import PythonOperator,SnowFlakeOperator,BashOperator,ShortCircuitOperator
import boto3
from airflow.utils.email import send_email
import time, re
from airflow.models import Variable
from sqlalchemy import func
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow import settings
from airflow.models import TaskInstance,DagRun
from airflow.utils.state import State

start_date = datetime.now()
task_concurrency=15
max_simultaneous_run = 1
schedule_interval='45 10 * * *'
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

dag = DAG(
    'dag_short_circuit', default_args=default_args, schedule_interval=schedule_interval,
    max_active_runs=max_simultaneous_run,
    concurrency=task_concurrency, start_date=start_date)

def check_dep_dag_success(*args,**kwargs):
    session = settings.Session()
    exec_date1 = session.query(func.max(DagRun.execution_date)).filter(DagRun.dag_id == 'dag_schedule_test1').scalar()
    exec_date2 = session.query(func.max(DagRun.execution_date)).filter(DagRun.dag_id == 'dag_schedule_test2').scalar()
    exec_date3 = session.query(func.max(DagRun.execution_date)).filter(DagRun.dag_id == 'dag_test').scalar()

    if exec_date1.date()==kwargs['execution_date'].date() and exec_date2.date()==kwargs['execution_date'].date() and exec_date3.date()==kwargs['execution_date'].date():
        status1=session.query(DagRun.state).filter(DagRun.dag_id == 'dag_schedule_test1',DagRun.execution_date==exec_date1).scalar()
        status2=session.query(DagRun.state).filter(DagRun.dag_id == 'dag_schedule_test2',DagRun.execution_date==exec_date2).scalar()
        status3=session.query(DagRun.state).filter(DagRun.dag_id == 'dag_test',DagRun.execution_date==exec_date3).scalar()
        if status1=="success" and status2=="success" and status3=="success":
            return True
        else:
            return False
    else:
        return False

    session.commit()
    session.close()

check_dag_state = ShortCircuitOperator(
         task_id='check_dag_state',
         provide_context=True,
         python_callable=check_dep_dag_success,
         dag=dag)

task_1_session2_py = BashOperator(
    task_id='task_1_session2_py',
    bash_command='python2.7 /opt/airflow/scripts/S2STG/DEV/session2.py',
    dag=dag
)


task_2_copy_to_snowflake_sql = SnowFlakeOperator(
    task_id='task_2_copy_to_snowflake_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/dags/2_copy_to_snowflake.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCHDR_D', 'TABLE_NAME_XI': 'LIKP_D_XI', 'PARAMTGTTABLE': 'LIKP_D_XI', 'SCHEMA_STG': 'EIS_STG_T', 'SRCSYS': "'R3_PRD'", 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/DEV', 'DATASRC': "'2LIS_12_VCHDR'", 'PSANAME': "'2LIS_12_VCHDR%'", 'BATCH_GROUP': 'USA', 'ORACLE_EXPORT_SQL': 'oracle_output1.sql', 'TABLE_NAME': 'DEV', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/DEV'}
)


task_3_GenerateNewAuditRow_sql = SnowFlakeOperator(
    task_id='task_3_GenerateNewAuditRow_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/dags/3_GenerateNewAuditRow.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCHDR_D', 'TABLE_NAME_XI': 'LIKP_D_XI', 'PARAMTGTTABLE': 'LIKP_D_XI', 'SCHEMA_STG': 'EIS_STG_T', 'SRCSYS': "'R3_PRD'", 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/DEV', 'DATASRC': "'2LIS_12_VCHDR'", 'PSANAME': "'2LIS_12_VCHDR%'", 'BATCH_GROUP': 'USA', 'ORACLE_EXPORT_SQL': 'oracle_output1.sql', 'TABLE_NAME': 'DEV', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/DEV'}
)


task_4_s_m_00_ACQUIRE_sql = SnowFlakeOperator(
    task_id='task_4_s_m_00_ACQUIRE_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/dags/4_s_m_00_ACQUIRE.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCHDR_D', 'TABLE_NAME_XI': 'LIKP_D_XI', 'PARAMTGTTABLE': 'LIKP_D_XI', 'SCHEMA_STG': 'EIS_STG_T', 'SRCSYS': "'R3_PRD'", 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/DEV', 'DATASRC': "'2LIS_12_VCHDR'", 'PSANAME': "'2LIS_12_VCHDR%'", 'BATCH_GROUP': 'USA', 'ORACLE_EXPORT_SQL': 'oracle_output1.sql', 'TABLE_NAME': 'DEV', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/DEV'}
)

check_dag_state.set_downstream(task_1_session2_py)
task_1_session2_py.set_downstream(task_2_copy_to_snowflake_sql)
task_2_copy_to_snowflake_sql.set_downstream(task_3_GenerateNewAuditRow_sql)
task_3_GenerateNewAuditRow_sql.set_downstream(task_4_s_m_00_ACQUIRE_sql)
