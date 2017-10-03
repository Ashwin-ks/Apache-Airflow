import jinja2 as jinja
import os, re, argparse
import logging
import json
import ConfigParser
import sys

# Create the jinja2 environment.
# Notice the use of trim_blocks, which greatly helps control whitespace.
j2_env = jinja.Environment(loader=jinja.FileSystemLoader("templates"),
                           trim_blocks=True)

global workflows_dir
global workflow_dir_prefix
global dag_output_dir
global s3_code_directory
global owner
app_name = "DAGgenerator"
logger = logging.getLogger(app_name)


# Main function
def get_task_params(file):
    print ("Param file name is " + file)
    config = ConfigParser.RawConfigParser()
    config.read(file)
    task_params = {}
    for each_section in config.sections():
        print ("each secton=" + each_section)
        section_params={}
        for (each_key, each_val) in config.items(each_section):
            print ("each key,each value = " + each_key.upper() , each_val)
            section_params.update({each_key.upper(): each_val})
        task_params.update({each_section: section_params})
    return task_params

def get_script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))


def get_success_audit_sql(tgt_table_name, actual_table_name, dbname, file_suffix, jobname):
    if file_suffix == "bteq":
        snippet = j2_env.get_template('BteqAudit.sql.jinja').render(
            STATUS='COMPLETE',
            SCRNME=jobname
        )
    else:
        snippet = j2_env.get_template('OnSuccess.sql.jinja').render(
            dbname=dbname,
            tgt_table_name=tgt_table_name,
            actual_table_name=actual_table_name
        )
    return snippet


def get_failure_audit_sql(tgt_table_name, actual_table_name, file_suffix, jobname):
    if file_suffix == "bteq":
        snippet = j2_env.get_template('BteqAudit.sql.jinja').render(
            STATUS='ERROR',
            SCRNME=jobname
        )
    else:
        snippet = j2_env.get_template('OnFailure.sql.jinja').render(
            dbname='EIS_T',
            tgt_table_name=tgt_table_name,
            actual_table_name=actual_table_name
        )
    return snippet


def main():
    global args
    global workflows_dir
    global workflow_dir_prefix
    global dag_output_dir
    global s3_code_directory
    global owner

    parser = argparse.ArgumentParser(description='Generating DAG for airflow')

    parser.add_argument('-w', '--workflowdir', dest='workflowdir', required=True,
                        help='Workflow Directory')
    parser.add_argument('-p', '--workflowprefix', dest='workflowprefix', required=True,
                        help='prefix for workflow files')
    parser.add_argument('-d', '--datoutputdir', dest='dagoutputdir', required=True,
                        help='Direcotry where generated workflows to be written')
    parser.add_argument('-s', '--s3codedir', dest='s3codedir', required=True,
                        help='code directory in s3')
    parser.add_argument('-o', '--owner', dest='owner', required=True,
                        help='owner of the workflow')

    args = parser.parse_args()

    workflows_dir = args.workflowdir  # "../code/workflows"
    workflow_dir_prefix = args.workflowprefix  # "wf_"
    dag_output_dir = args.dagoutputdir  # "../generated-dags"
    s3_code_directory = args.s3codedir  # "s3://boom/boom/boom"
    owner = args.owner  # "EDF"
    # non param variables

    for dir in os.walk(workflows_dir).next()[1]:
        script_files = []
        task_params_dict = {"": None}
        if dir.startswith(workflow_dir_prefix):
            workflow_name = dir.split(workflow_dir_prefix)[1]
            file_suffix = ""
            for file in sorted(os.listdir(os.path.join(workflows_dir, dir))):
                # Consider only those that starts with <number>_
                # TODO: Replace all prints with logging
                # logger.info("Workflow : %s - %s" % (workflow_name, file))
                print "Workflow : %s - %s" % (workflow_name, file)
                if re.match("[0-9]+_", file):
                    script_files.append(file)
                    file_suffix = file.rpartition('.')[2]
                elif re.match("PARAMFILE", file):
                    task_params_dict.clear()
                    task_params_dict.update(get_task_params(workflows_dir + '/' + dir + '/' + file))
                    print 'printing the dags params', task_params_dict
            # Generate DAG files for each workflow
            for profile, params in task_params_dict.items():
                dag_name=workflow_name
                if len(profile) > 0 and "args" != profile.strip():
                    dag_name=workflow_name + '_' + profile.strip()
                # Dirty trick to to support delimitation with bot '.' and '$!'

                if params is None or "args" == profile.strip():
                    tgt_table_name = re.split('\.|\$!', dag_name)[1]
                    actual_table_name = re.split('\.|\$!', dag_name)[1]
                    dbname = 'EIS_T'
                else:
                    tgt_table_name = params['TGT_REGION_TABLE']
                    actual_table_name = params['TABLE_NAME_IN_AUDIT']
                    dbname = params['DBNAME']

                print ('tgt_table_name' + tgt_table_name)
                if file_suffix == "sql":
                    success_audit_sql=get_success_audit_sql(tgt_table_name, actual_table_name, dbname, file_suffix, "None")
                    failure_audit_sql=get_failure_audit_sql(tgt_table_name, actual_table_name, file_suffix, "None")
                    generate_dag(dag_name=dag_name, script_files=script_files,
                                 dag_template="MainDagTemplate.py.jinja", task_params=params, success_audit_sql=success_audit_sql, failure_audit_sql=failure_audit_sql, workflow_name=workflow_name)
                elif file_suffix == "tpt":
                    success_audit_sql = get_success_audit_sql(tgt_table_name, actual_table_name, dbname, file_suffix, "None")
                    failure_audit_sql = get_failure_audit_sql(tgt_table_name, actual_table_name, file_suffix, "None")
                    generate_dag(dag_name=dag_name, script_files=script_files,
                                 dag_template="MainDagTptTemplate.py.jinja", task_params=params, success_audit_sql=success_audit_sql, failure_audit_sql=failure_audit_sql, workflow_name=workflow_name)
                elif file_suffix == "bteq":
                    jobname = params["SCRNME"]
                    success_audit_sql=get_success_audit_sql(tgt_table_name, actual_table_name, dbname, file_suffix, jobname)
                    failure_audit_sql=get_failure_audit_sql(tgt_table_name, actual_table_name, file_suffix, jobname)
                    generate_dag(dag_name=dag_name, script_files=script_files,
                                 dag_template="MainDagBteqTemplate.py.jinja", task_params=params, success_audit_sql=success_audit_sql, failure_audit_sql=failure_audit_sql, workflow_name=workflow_name)


def generate_operator_snippet(script_file, task_params, workflow_name):
    snippet = ""
    # Handle Snowflake SQL
    if script_file.endswith(".sql") or script_file.endswith(".tpt") or script_file.endswith(".bteq"):
        snippet = j2_env.get_template('SnowflakeOperator.py.jinja').render(
            task_id=get_task_id(script_file),
            sql_file=s3_code_directory + '/' + workflow_dir_prefix + workflow_name + '/' + script_file,
            conn_id='snowflake',
            env='dev',
            parameters=dict((k,v) for k,v in task_params.iteritems() if k not in ('START_DATE','SCHEDULE_INTERVAL','TRIGGER_DAG_ID'))
        )
    # TODO: Add other operators based on other file extentions (e.g. .hql for hive). Any unknown extention shoud be skipped
    return snippet;

def generate_trigoperator_snippet(task_params,all_tasks):
    snippet=j2_env.get_template('Stg2TgtSessionDagRunOperator.py.jinja').render(
                TRIGGER_DAG_ID=task_params['TRIGGER_DAG_ID'],
                DEPENDANT_TASK_NAME=all_tasks 
    )
    return snippet;

def generate_concentrator_snippet(index, workflow_name):
    snippet = j2_env.get_template('JoinTaskTemplate.py.jinja').render(
        dag=workflow_name,
        join_index=index
    )
    return snippet;

def generate_runcontrol_snippet(condition_eval_code,code_type,on_continue_task_id):
    snippet = j2_env.get_template('RunControlTemplate.py.jinja').render(
        condition_eval_code=condition_eval_code,
        code_type=code_type,
        on_continue_task_id=on_continue_task_id
    )
    return snippet;

def generate_set_downstream_snippet(source, target):
    snippet = j2_env.get_template('SetDownstreamTemplate.py.jinja').render(
        source=source,
        target=target
    )
    return snippet;


def generate_dag(dag_name, script_files, dag_template, task_params, success_audit_sql, failure_audit_sql, workflow_name):
    if 'START_DATE' in task_params:
	start_date=task_params['START_DATE']
    else:
	start_date='datetime.now()'
    if 'SCHEDULE_INTERVAL' in task_params:
	schedule_interval=task_params['SCHEDULE_INTERVAL']
    else:
	schedule_interval='None'
    dag = j2_env.get_template(dag_template).render(
        owner=owner,
        dag_name=dag_name.replace(".", "_"), success_audit_sql=success_audit_sql, failure_audit_sql=failure_audit_sql, tgt_table_name=dag_name.split('.')[1],start_date=start_date,schedule_interval=schedule_interval)
    # task_order_details is a dict of position and list of tasks in that position level
    task_order_details = {}
    for script_file in script_files:
        file_details = parse_task_position(script_file)

        # file_details[0] = position of the task in the dag
        tl = task_order_details.get(file_details[0], [])
        tl.append(get_task_id(script_file))
        task_order_details.update({

            file_details[0]: tl
        })
        dag += generate_operator_snippet(script_file, task_params, workflow_name)
        print task_order_details

    all_tasks=list()
    for taskid in task_order_details.values():
        all_tasks.extend(taskid)
    all_tasks.append('finished_all')
    all_tasks=str(all_tasks).replace('[','').replace(']','').replace(',',' and ').replace('"','')
    print all_tasks

    for i in range(1, len(task_order_details.keys()) + 1):
	if i == len(task_order_details.keys()) and "TRIGGER_DAG_ID" in task_params:
	    dag+=generate_trigoperator_snippet(task_params,all_tasks)


    run_control=get_run_control(workflow_name)
    #run_control={"condition_eval_code" : "SELECT TRUE", "code_type" : "sql"}
    if run_control:
        dag += generate_runcontrol_snippet(run_control.get("condition_eval_code"), run_control.get("code_type"),task_order_details.get("1")[0])
        dag += generate_set_downstream_snippet("run_controller",task_order_details.get("1")[0])

    # Now set up and downstreams
    for i in range(1, len(task_order_details.keys()) + 1):
        task_list = task_order_details.get(str(i))
        if len(task_list) > 1:
            if i < len(task_order_details.keys()):
                dag += generate_concentrator_snippet(i, dag_name)
                for next_task in task_order_details.get(str(i + 1)):
                    dag += generate_set_downstream_snippet("join_task_" + str(i), next_task)

        for task in task_list:
            if len(task_list) > 1:
                if i == len(task_order_details.keys()):
                    dag += generate_set_downstream_snippet(task, "finished_all")
                else:
                    dag += generate_set_downstream_snippet(task, "join_task_" + str(i))
		
		if i == len(task_order_details.keys()) and 'TRIGGER_DAG_ID' in task_params:
                    dag += generate_set_downstream_snippet(task, "trigger")

            elif len(task_list) == 1:
                if i == len(task_order_details.keys()):
                    dag += generate_set_downstream_snippet(task, "finished_all")
		elif i == len(task_order_details.keys()) and 'TRIGGER_DAG_ID' in task_params:
		    dag += generate_set_downstream_snippet("finished_all", "trigger")
                elif i < len(task_order_details.keys()):
                    for next_task in task_order_details.get(str(i + 1)):
                        dag += generate_set_downstream_snippet(task, next_task)

		if i == len(task_order_details.keys()) and 'TRIGGER_DAG_ID' in task_params:
                    dag += generate_set_downstream_snippet("finished_all", "trigger")

    check_out_dir = dag_output_dir
    if not os.path.exists(check_out_dir):
        os.makedirs(check_out_dir)
    # For loop here to generate many dags if PARAMFILE is present

    with open(os.path.join(check_out_dir, "dag_" + dag_name.replace(".", "_") + ".py"), 'w') as dagfile:
        dagfile.write(dag)
        dagfile.close()

def get_run_control(workflow_name):
    config = ConfigParser.RawConfigParser()
    xcom_infile = "%s/%s%s/RUNCONTROLFILE" % (workflows_dir, workflow_dir_prefix, workflow_name)
    config.read(xcom_infile)
    if config.has_section("default"):
        return dict(config.items("default"))
    else:
        return {}

def parse_task_position(sqlfile_name):
    position_search = re.search("([0-9]+)+_(.*)", sqlfile_name, re.IGNORECASE)
    if position_search:
        name = position_search.group(2)
        position = position_search.group(1)
    return (position, name)


def get_task_id(script_name=""):
    return "task_"+script_name.replace(".", "_")


if __name__ == "__main__":
    main()
