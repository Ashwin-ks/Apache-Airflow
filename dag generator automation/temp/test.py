import datetime
from airflow import settings
from airflow.models import TaskInstance
from airflow.models import DagRun
from airflow.utils.state import State

session = settings.Session()
success_runs = session.query(TaskInstance).filter(TaskInstance.state == State.SUCCESS).all()
#print(success_runs)
status_dagrun=session.query(DagRun.state).filter(DagRun.dag_id=='dag_S2STG_EIS_STG_T_Consolrtm_ACQUIRE_PRD_S1').filter(DagRun.execution_date>'2017-06-20').order_by(-DagRun.id).all()
print(len(status_dagrun))
session.close()
