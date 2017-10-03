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

dag = DAG(
    'dag_short_circuit_check', default_args=default_args, schedule_interval=schedule_interval,
    max_active_runs=max_simultaneous_run,
    concurrency=task_concurrency, start_date=start_date)

def check_exec_date(id):
    session = settings.Session()
    exec_date = session.query(func.max(DagRun.execution_date)).filter(DagRun.dag_id == id).scalar()
    return exec_date

def check_dag_state(id):
    session = settings.Session()
    status = session.query(DagRun.state).filter(DagRun.dag_id == id,DagRun.execution_date==check_exec_date(id)).scalar()
    return status

def check_dep_dag_state(*args,**kwargs):
    try:
        session = settings.Session()
        logging.info('Checking dependant dags status')
        exec_dates = [check_exec_date(dag) for dag in ('dag_schedule_test1','dag_schedule_test2','dag_test')]
        if all(dag_date.date()==kwargs['execution_date'].date() for dag_date in exec_dates):
            dag_state_all=[check_dag_state(dag) for dag in ('dag_schedule_test1','dag_schedule_test2','dag_test')]
            if all(state=="success" for state in dag_state_all):
                return True
            elif any(state=="running" for state in dag_state_all):
                while any(state=="running" for state in dag_state_all):
                    logging.info('Checking running dags status after 120 seconds')
                    time.sleep(120)
                    dag_state_all=[check_dag_state(dag) for dag in ('dag_schedule_test1','dag_schedule_test2','dag_test')]
                    if any(state=="failed" for state in dag_state_all):
                        logging.info('dependant dags scenario not met,skipping downstream tasks')
                        return False
                    elif all(state=="success" for state in dag_state_all):
                        return True
        else:
            logging.info('dependant dags scenario not met,skipping downstream tasks')
            return False
    except:
            return False

    session.commit()
    session.close()

check_prev_dag_state = ShortCircuitOperator(
         task_id='check_dag_state_prev',
         provide_context=True,
         python_callable=check_dep_dag_state,
         dag=dag)

task_1_session2_OutbndDlvryHdr_py = BashOperator(
    task_id='task_1_session2_OutbndDlvryHdr_py',
    bash_command='python2.7 /opt/airflow/scripts/S2STG/OutBndDlvryHdr_DEV/session2.py',
    dag=dag
)


task_2_copy_to_snowflake_OutbndDlvryHdr_sql = SnowFlakeOperator(
    task_id='task_2_copy_to_snowflake_OutbndDlvryHdr_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/dags/2_copy_to_snowflake_OutbndDlvryHdr.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCHDR_D', 'TABLE_NAME_XI': 'LIKP_D_XI', 'PARAMTGTTABLE': 'LIKP_D_XI', 'SCHEMA_STG': 'EIS_STG_T', 'SRCSYS': "'R3_PRD'", 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/OutBndDlvryHdr_DEV', 'DATASRC': "'2LIS_12_VCHDR'", 'PSANAME': "'2LIS_12_VCHDR%'", 'BATCH_GROUP': 'USA', 'ORACLE_EXPORT_SQL': 'oracle_output1.sql', 'TABLE_NAME': 'OutBndDlvryHdr_DEV', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/OutBndDlvryHdr_DEV'}
)


task_3_GenerateNewAuditRow_OutbndDlvryHdr_sql = SnowFlakeOperator(
    task_id='task_3_GenerateNewAuditRow_OutbndDlvryHdr_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/dags/3_GenerateNewAuditRow_OutbndDlvryHdr.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCHDR_D', 'TABLE_NAME_XI': 'LIKP_D_XI', 'PARAMTGTTABLE': 'LIKP_D_XI', 'SCHEMA_STG': 'EIS_STG_T', 'SRCSYS': "'R3_PRD'", 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/OutBndDlvryHdr_DEV', 'DATASRC': "'2LIS_12_VCHDR'", 'PSANAME': "'2LIS_12_VCHDR%'", 'BATCH_GROUP': 'USA', 'ORACLE_EXPORT_SQL': 'oracle_output1.sql', 'TABLE_NAME': 'OutBndDlvryHdr_DEV', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/OutBndDlvryHdr_DEV'}
)


task_4_s_m_OUTBNDDLVRYHDR_00_ACQUIRE_sql = SnowFlakeOperator(
    task_id='task_4_s_m_OUTBNDDLVRYHDR_00_ACQUIRE_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/dev/var/dags/4_s_m_OUTBNDDLVRYHDR_00_ACQUIRE.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCHDR_D', 'TABLE_NAME_XI': 'LIKP_D_XI', 'PARAMTGTTABLE': 'LIKP_D_XI', 'SCHEMA_STG': 'EIS_STG_T', 'SRCSYS': "'R3_PRD'", 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/OutBndDlvryHdr_DEV', 'DATASRC': "'2LIS_12_VCHDR'", 'PSANAME': "'2LIS_12_VCHDR%'", 'BATCH_GROUP': 'USA', 'ORACLE_EXPORT_SQL': 'oracle_output1.sql', 'TABLE_NAME': 'OutBndDlvryHdr_DEV', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/OutBndDlvryHdr_DEV'}
)

check_prev_dag_state.set_downstream(task_1_session2_OutbndDlvryHdr_py)
task_1_session2_OutbndDlvryHdr_py.set_downstream(task_2_copy_to_snowflake_OutbndDlvryHdr_sql)
task_2_copy_to_snowflake_OutbndDlvryHdr_sql.set_downstream(task_3_GenerateNewAuditRow_OutbndDlvryHdr_sql)
task_3_GenerateNewAuditRow_OutbndDlvryHdr_sql.set_downstream(task_4_s_m_OUTBNDDLVRYHDR_00_ACQUIRE_sql)