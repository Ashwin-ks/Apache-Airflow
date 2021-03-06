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
    'S2STG_EIS_STG_T_OutBndDlvryHdr_PRD_S1_D', default_args=default_args, schedule_interval=schedule_interval,
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
	exec_dates = [check_exec_date(dag) for dag in ['dag_schedule_test1', 'dag_schedule_test2', 'dag_test']]
	#if all(dag_date.date()==kwargs['execution_date'].date() for dag_date in exec_dates):
	dag_state_all=[check_dag_state(dag) for dag in ['dag_schedule_test1', 'dag_schedule_test2', 'dag_test']]
	if all(state=="success" for state in dag_state_all):
            logging.info(dag_state_all)
	    return True
	elif any(state=="running" for state in dag_state_all):
	    while any(state=="running" for state in dag_state_all):
	        logging.info('Checking running dags status after 5 minutes')
		time.sleep(300)
		dag_state_all=[check_dag_state(dag) for dag in ['dag_schedule_test1', 'dag_schedule_test2', 'dag_test']]
		logging.info(dag_state_all)   
                if any(state=="failed" for state in dag_state_all):
		    logging.info('dependant dags scenario not met,skipping downstream tasks')
		    return False
		elif all(state=="success" for state in dag_state_all):
		    return True
    except:
	    return False
		
    session.commit()
    session.close()
	
check_prev_dag_state = ShortCircuitOperator(
         task_id='check_dag_state_prev',
         provide_context=True,
         python_callable=check_dep_dag_state,
         dag=dag)


task_1_select_snowflake_OutBndDlvryHdr_PRD_sql = SnowFlakeOperator(
    task_id='task_1_select_snowflake_OutBndDlvryHdr_PRD_sql',
    sql="s3:/home/dev/compy.py/wf_S2STG_EIS_STG_T_OutBndDlvryHdr_PRD_S1/1_select_snowflake_OutBndDlvryHdr_PRD.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCHDR_D', 'TABLE_NAME_XI': 'LIKP_D_XI', 'PARAMTGTTABLE': 'LIKP_D_XI', 'SCHEMA_STG': 'EIS_STG_T', 'DATABASE': 'NGP_DA_DEV', 'SRCSYS': "'R3_PRD'", 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/OutBndDlvryHdr_PRD', 'DATASRC': "'2LIS_12_VCHDR'", 'PSANAME': "'2LIS_12_VCHDR%'", 'BATCH_GROUP': 'USA', 'TABLE_NAME': 'OutBndDlvryHdr_PRD', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/OutBndDlvryHdr_PRD'}
)


task_2_select_snowflake_OutBndDlvryHdr_PRD_sql = SnowFlakeOperator(
    task_id='task_2_select_snowflake_OutBndDlvryHdr_PRD_sql',
    sql="s3:/home/dev/compy.py/wf_S2STG_EIS_STG_T_OutBndDlvryHdr_PRD_S1/2_select_snowflake_OutBndDlvryHdr_PRD.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCHDR_D', 'TABLE_NAME_XI': 'LIKP_D_XI', 'PARAMTGTTABLE': 'LIKP_D_XI', 'SCHEMA_STG': 'EIS_STG_T', 'DATABASE': 'NGP_DA_DEV', 'SRCSYS': "'R3_PRD'", 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/OutBndDlvryHdr_PRD', 'DATASRC': "'2LIS_12_VCHDR'", 'PSANAME': "'2LIS_12_VCHDR%'", 'BATCH_GROUP': 'USA', 'TABLE_NAME': 'OutBndDlvryHdr_PRD', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/OutBndDlvryHdr_PRD'}
)

task_3_session1_OutBndDlvryHdr_PRD_py = BashOperator(
    task_id='task_3_session1_OutBndDlvryHdr_PRD_py',
    bash_command='python2.7 /opt/airflow/scripts/S2STG/OutBndDlvryHdr_PRD/3_session1_OutBndDlvryHdr_PRD.py',
    dag=dag
)


task_4_copy_to_snowflake_OutBndDlvryHdr_PRD_sql = SnowFlakeOperator(
    task_id='task_4_copy_to_snowflake_OutBndDlvryHdr_PRD_sql',
    sql="s3:/home/dev/compy.py/wf_S2STG_EIS_STG_T_OutBndDlvryHdr_PRD_S1/4_copy_to_snowflake_OutBndDlvryHdr_PRD.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCHDR_D', 'TABLE_NAME_XI': 'LIKP_D_XI', 'PARAMTGTTABLE': 'LIKP_D_XI', 'SCHEMA_STG': 'EIS_STG_T', 'DATABASE': 'NGP_DA_DEV', 'SRCSYS': "'R3_PRD'", 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/OutBndDlvryHdr_PRD', 'DATASRC': "'2LIS_12_VCHDR'", 'PSANAME': "'2LIS_12_VCHDR%'", 'BATCH_GROUP': 'USA', 'TABLE_NAME': 'OutBndDlvryHdr_PRD', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/OutBndDlvryHdr_PRD'}
)


task_5_DeleteSRC_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql = SnowFlakeOperator(
    task_id='task_5_DeleteSRC_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql',
    sql="s3:/home/dev/compy.py/wf_S2STG_EIS_STG_T_OutBndDlvryHdr_PRD_S1/5_DeleteSRC_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD.sql ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'PARAMSRCTABLE': 'S_2LIS_12_VCHDR_D', 'TABLE_NAME_XI': 'LIKP_D_XI', 'PARAMTGTTABLE': 'LIKP_D_XI', 'SCHEMA_STG': 'EIS_STG_T', 'DATABASE': 'NGP_DA_DEV', 'SRCSYS': "'R3_PRD'", 'INTERMEDIATE_FILE_PATH': '/opt/airflow/s2stg_wrk/OutBndDlvryHdr_PRD', 'DATASRC': "'2LIS_12_VCHDR'", 'PSANAME': "'2LIS_12_VCHDR%'", 'BATCH_GROUP': 'USA', 'TABLE_NAME': 'OutBndDlvryHdr_PRD', 'SCHEMA_T': 'EIS_T', 'SCRIPT_PATH': '/opt/airflow/scripts/S2STG/OutBndDlvryHdr_PRD'}
)

trigger1 = TriggerDagRunOperator(task_id='trigger_dagrun1',
                                 trigger_dag_id="S2STG_EIS_STG_T_OutBndDlvryHdr_PRD_S2_D",
                                 python_callable=conditionally_trigger,
                                 params={
                                        'dependant_task_name':'task_1_select_snowflake_OutBndDlvryHdr_PRD_sql' and  'task_3_session1_OutBndDlvryHdr_PRD_py' and  'task_2_select_snowflake_OutBndDlvryHdr_PRD_sql' and  'task_5_DeleteSRC_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql' and  'task_4_copy_to_snowflake_OutBndDlvryHdr_PRD_sql'},
                                 dag=dag)


trigger2 = TriggerDagRunOperator(task_id='trigger_dagrun2',
                                 trigger_dag_id="S2STG_EIS_STG_T_OutBndDlvryHdr_PRD_S2_A",
                                 python_callable=conditionally_trigger,
                                 params={
                                        'dependant_task_name':'task_1_select_snowflake_OutBndDlvryHdr_PRD_sql' and  'task_3_session1_OutBndDlvryHdr_PRD_py' and  'task_2_select_snowflake_OutBndDlvryHdr_PRD_sql' and  'task_5_DeleteSRC_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql' and  'task_4_copy_to_snowflake_OutBndDlvryHdr_PRD_sql'},
                                 dag=dag)


trigger3 = TriggerDagRunOperator(task_id='trigger_dagrun3',
                                 trigger_dag_id="S2STG_EIS_STG_T_OutBndDlvryHdr_PRD_S2_E",
                                 python_callable=conditionally_trigger,
                                 params={
                                        'dependant_task_name':'task_1_select_snowflake_OutBndDlvryHdr_PRD_sql' and  'task_3_session1_OutBndDlvryHdr_PRD_py' and  'task_2_select_snowflake_OutBndDlvryHdr_PRD_sql' and  'task_5_DeleteSRC_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql' and  'task_4_copy_to_snowflake_OutBndDlvryHdr_PRD_sql'},
                                 dag=dag)


check_prev_dag_state.set_downstream(task_1_select_snowflake_OutBndDlvryHdr_PRD_sql)
task_1_select_snowflake_OutBndDlvryHdr_PRD_sql.set_downstream(task_2_select_snowflake_OutBndDlvryHdr_PRD_sql)
task_2_select_snowflake_OutBndDlvryHdr_PRD_sql.set_downstream(task_3_session1_OutBndDlvryHdr_PRD_py)
task_3_session1_OutBndDlvryHdr_PRD_py.set_downstream(task_4_copy_to_snowflake_OutBndDlvryHdr_PRD_sql)
task_4_copy_to_snowflake_OutBndDlvryHdr_PRD_sql.set_downstream(task_5_DeleteSRC_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql)
task_5_DeleteSRC_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql.set_downstream(trigger1)
task_5_DeleteSRC_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql.set_downstream(trigger2)
task_5_DeleteSRC_s_m_OutBndDlvryHdr_00_ACQUIRE_PRD_sql.set_downstream(trigger3)