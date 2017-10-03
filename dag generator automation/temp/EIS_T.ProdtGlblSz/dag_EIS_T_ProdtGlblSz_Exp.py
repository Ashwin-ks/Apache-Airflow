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

raw_bucket = Variable.get("raw_bucket")
env = Variable.get("env")
table_name = ''
start_date = datetime.now()
task_concurrency=15
max_simultaneous_run = 1
schedule_interval=None
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
    'EIS_T_ProdtGlblSz_Exp', default_args=default_args, schedule_interval=schedule_interval,
    max_active_runs=max_simultaneous_run,
    concurrency=task_concurrency, start_date=start_date)

def archiving_s3_files(**kwargs):
    logging.info("snowflake table processing" + table_name)
    logging.info("env is" + env)
    logging.info("raw bucket is " + raw_bucket)
    s3 = boto3.resource('s3')
    base_bucket = s3.Bucket(raw_bucket)
    landing_path = "%s/FileLanding/FullLoad/MasterData/EDW/booking-%s/" %(env,table_name)
    archive_path = "%s/Archiving/FullLoad/MasterData/EDW/booking-%s/" %(env,table_name)
    logging.info("landing path is " + landing_path)
    logging.info("archiving path is " + archive_path)

    try:
        for obj in base_bucket.objects.filter(Prefix=landing_path):
            #print str(obj.key).endswith(".gz")
            if str(obj.key).endswith(".gz"):
                s3.Object(raw_bucket, archive_path + str(obj.key).split('/')[-1]).copy_from(CopySource=raw_bucket + '/' + str(obj.key))
                logging.info("archiving s3 file " + archive_path + str(obj.key).split('/')[-1])
        logging.info("copying done into archiving")
        #Deleting file from s3 filelanding
        for obj in base_bucket.objects.filter(Prefix=landing_path):
            if str(obj.key).endswith(".gz"):
                s3.Object(raw_bucket,str(obj.key)).delete()
        logging.info("deleting done")
    except Exception as e:
        logging.exception(e)
        raise e

def failure_callback(context):
    pass

# This is the end node
finished_all = PythonOperator(
    task_id='finished_all',
    python_callable=archiving_s3_files,
    dag=dag)


exp_user=Variable.get("export_user")
exp_host=Variable.get("export_host")
exp_subscriberid=Variable.get("export_subscriberid")
exp_datasetid=Variable.get("export_datasetid")

export_bash_command='ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no {USER}@{HOST} "sh /home/{USER}/scripts/fullload/1_digenexport_full.ksh {SUBSCRIBERID} {DATASETID}"'.format(USER=exp_user,HOST=exp_host,SUBSCRIBERID=exp_subscriberid,DATASETID=exp_datasetid)

task_1_digenexport_full_ksh = BashOperator(
    task_id='task_1_digenexport_full_ksh',
    bash_command=export_bash_command,
    dag=dag
)


task_2_Copy_Merge_ProdtGlblSz_tpt = SnowFlakeOperator(
    task_id='task_2_Copy_Merge_ProdtGlblSz_tpt',
    sql="rev-logistics-ingest/code/workflows//wf_EIS_T.ProdtGlblSz/2_Copy_Merge_ProdtGlblSz.tpt ",
    conn_id="snowflake",
    dag=dag,
    on_failure_callback=None,
    parameters={'SCHEMA_T': 'EIS_T', 'WAREHOUSE': 'REV_LOGISTICS_EIS_T', 'TABLE_NAME': 'ProdtGlblSz'}
)

task_1_digenexport_full_ksh.set_downstream(task_2_Copy_Merge_ProdtGlblSz_tpt)
task_2_Copy_Merge_ProdtGlblSz_tpt.set_downstream(finished_all)