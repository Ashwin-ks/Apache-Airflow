def main():
    import os, re, argparse
    global dag_id
    global date
    parser = argparse.ArgumentParser(description='Trigger Stage to Target Dags')
    parser.add_argument('-id', '--dagid', dest='dagid', required=True,
                        help='dagid')
    parser.add_argument('-date', '--dateofexecution', dest='dateofexecution', required=True,
                        help='dateofexecution')
    args = parser.parse_args()
    dagid = args.dagid  
    dateofexecution = args.dateofexecution 
    import datetime
    from airflow import settings
    from airflow.models import DagRun
    from airflow.utils.state import State
    session = settings.Session()
    status_dagrun=session.query(DagRun.state).filter(DagRun.dag_id==dagid).filter(DagRun.execution_date>dateofexecution).order_by(-DagRun.id).all()
    #print(status_dagrun)
    return (len(status_dagrun))
if __name__ == "__main__":
    print (main())
