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
    'S2STG_EIS_STG_T_InOutBndShpmtItm_ACQUIRE_S1_A', default_args=default_args, schedule_interval=schedule_interval,
    max_active_runs=max_simultaneous_run,
    concurrency=task_concurrency, start_date=start_date)



task_1_select_snowflake_InOutBndShpmtItm_sql = SnowFlakeOperator(
    task_id='task_1_select_snowflake_InOutBndShpmtItm_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/lab/var/workflows/wf_S2STG_EIS_STG_T_InOutBndShpmtItm_ACQUIRE_S1/1_select_snowflake_InOutBndShpmtItm.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_08TRTLP_A', 'TABLE_NAME_XI': 'VTTP_A_xi', 'PARAMTGTTABLE': 'VTTP_A_xi', 'SRCSYS': "'R3_PRA'", 'SCHEMA_STG_T': 'EIS_STG_T', 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/InOutBndShpmtItm_PRA', 'DATASRC': "'2LIS_08TRTLP'", 'PSANAME': "'2LIS_08TRTLP%'", 'BATCH_GROUP': 'ASIA', 'TABLE_NAME': 'InOutBndShpmtItm_PRA', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/S2STG/InOutBndShpmtItm_PRA'}
)


task_2_select_snowflake_InOutBndShpmtItm_sql = SnowFlakeOperator(
    task_id='task_2_select_snowflake_InOutBndShpmtItm_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/lab/var/workflows/wf_S2STG_EIS_STG_T_InOutBndShpmtItm_ACQUIRE_S1/2_select_snowflake_InOutBndShpmtItm.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_08TRTLP_A', 'TABLE_NAME_XI': 'VTTP_A_xi', 'PARAMTGTTABLE': 'VTTP_A_xi', 'SRCSYS': "'R3_PRA'", 'SCHEMA_STG_T': 'EIS_STG_T', 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/InOutBndShpmtItm_PRA', 'DATASRC': "'2LIS_08TRTLP'", 'PSANAME': "'2LIS_08TRTLP%'", 'BATCH_GROUP': 'ASIA', 'TABLE_NAME': 'InOutBndShpmtItm_PRA', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/S2STG/InOutBndShpmtItm_PRA'}
)

task_3_session1_InOutBndShpmtItm_py = BashOperator(
    task_id='task_3_session1_InOutBndShpmtItm_py',
    bash_command='python2.7 /opt/airflow/S2STG/InOutBndShpmtItm_PRA/3_session1_InOutBndShpmtItm.py',
    dag=dag
)


task_4_copy_to_snowflake_InOutBndShpmtItm_sql = SnowFlakeOperator(
    task_id='task_4_copy_to_snowflake_InOutBndShpmtItm_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/lab/var/workflows/wf_S2STG_EIS_STG_T_InOutBndShpmtItm_ACQUIRE_S1/4_copy_to_snowflake_InOutBndShpmtItm.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_08TRTLP_A', 'TABLE_NAME_XI': 'VTTP_A_xi', 'PARAMTGTTABLE': 'VTTP_A_xi', 'SRCSYS': "'R3_PRA'", 'SCHEMA_STG_T': 'EIS_STG_T', 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/InOutBndShpmtItm_PRA', 'DATASRC': "'2LIS_08TRTLP'", 'PSANAME': "'2LIS_08TRTLP%'", 'BATCH_GROUP': 'ASIA', 'TABLE_NAME': 'InOutBndShpmtItm_PRA', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/S2STG/InOutBndShpmtItm_PRA'}
)


task_5_DeleteSRC_s_m_InOutBndShpmtItm_00_ACQUIRE_sql = SnowFlakeOperator(
    task_id='task_5_DeleteSRC_s_m_InOutBndShpmtItm_00_ACQUIRE_sql',
    sql="s3://edf-infrabootstrap-preprod/airflow/revlogistics/lab/var/workflows/wf_S2STG_EIS_STG_T_InOutBndShpmtItm_ACQUIRE_S1/5_DeleteSRC_s_m_InOutBndShpmtItm_00_ACQUIRE.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_08TRTLP_A', 'TABLE_NAME_XI': 'VTTP_A_xi', 'PARAMTGTTABLE': 'VTTP_A_xi', 'SRCSYS': "'R3_PRA'", 'SCHEMA_STG_T': 'EIS_STG_T', 'DATABASE': 'NGP_DA_DEV', 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/InOutBndShpmtItm_PRA', 'DATASRC': "'2LIS_08TRTLP'", 'PSANAME': "'2LIS_08TRTLP%'", 'BATCH_GROUP': 'ASIA', 'TABLE_NAME': 'InOutBndShpmtItm_PRA', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/S2STG/InOutBndShpmtItm_PRA'}
)

trigger = TriggerDagRunOperator(task_id='trigger_dagrun',
                                 trigger_dag_id="S2STG_EIS_STG_T_InOutBndShpmtItm_ACQUIRE_S2_A_A",
                                 python_callable=conditionally_trigger,
                                 params={
                                        'dependant_task_name':'task_1_select_snowflake_InOutBndShpmtItm_sql' and  'task_3_session1_InOutBndShpmtItm_py' and  'task_2_select_snowflake_InOutBndShpmtItm_sql' and  'task_5_DeleteSRC_s_m_InOutBndShpmtItm_00_ACQUIRE_sql' and  'task_4_copy_to_snowflake_InOutBndShpmtItm_sql'},
                                 dag=dag)

task_1_select_snowflake_InOutBndShpmtItm_sql.set_downstream(task_2_select_snowflake_InOutBndShpmtItm_sql)
task_2_select_snowflake_InOutBndShpmtItm_sql.set_downstream(task_3_session1_InOutBndShpmtItm_py)
task_3_session1_InOutBndShpmtItm_py.set_downstream(task_4_copy_to_snowflake_InOutBndShpmtItm_sql)
task_4_copy_to_snowflake_InOutBndShpmtItm_sql.set_downstream(task_5_DeleteSRC_s_m_InOutBndShpmtItm_00_ACQUIRE_sql)
task_5_DeleteSRC_s_m_InOutBndShpmtItm_00_ACQUIRE_sql.set_downstream(trigger)