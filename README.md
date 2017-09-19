# Apache-Airflow
Airflow is a workflow management built by Airbnb. Complex workflows are defined using python code which represent DAG(Directed Acyclic Graphs). DAG's define tasks using operators,handles smart scheduling and dependencies among tasks executions as well as failures and retries.
It is open-sourced and uses python code to define tasks and their dependencies, executes those tasks on a regular schedule, and distributes task execution across worker processes

## Workflows
Workflows in Airflow are collections of tasks that have directional dependencies. Specifically, Airflow uses directed acyclic graphs — or DAG for short — to represent a workflow. Each node in the graph is a task, and edges define dependencies amongst tasks

![UI DAG Graph View](https://user-images.githubusercontent.com/19341622/30603503-52187db6-9d85-11e7-9315-2b8c6bbca04a.png)

### Why Airflow
Generally data workflows have complex dependency relationships among the tasks and requires scheduling ,Airflow handles smart scheduling and dependencies among tasks executions as well as failures and retries.
Airflow provides a feature rich UI with a wide range of functionality, allowing one to monitor multiple sources of metadata including execution logs, task states, landing times, task durations and even manage the states of workflows.
The Airflow webserver UI displays the states of currently active and past tasks, shows diagnostic information about task execution and makes it easy to monitor and troubleshoot workflows.
Airflow is open sourced and implemented in Python.

## Airflow Architecture
At its core Airflow has four primary components:-
Metadata Database
Scheduler
Executor
Workers


Airflow’s operation is built atop a Metadata Database which stores the state of tasks and workflows (i.e. DAGs). The Scheduler and Executor send tasks to a queue for Worker processes to perform. The Webserver runs (often-times running on the same machine as the Scheduler) and communicates with the database to render task state and Task Execution Logs in the Web UI

Metadata Database: this database stores information regarding the state of tasks. Database updates are performed using an abstraction layer implemented in SQLAlchemy. This abstraction layer cleanly separates the function of the remaining components of Airflow from the database.
Scheduler: The Scheduler is a process that uses DAG definitions in conjunction with the state of tasks in the metadata database to decide which tasks need to be executed, as well as their execution priority. The Scheduler is generally run as a service. 
Executor: The Executor is a message queuing process that is tightly bound to the Scheduler and determines the worker processes that actually execute each scheduled task. There are different types of Executors, each of which uses a specific class of worker processes to execute tasks. For example, the LocalExecutor executes tasks with parallel processes that run on the same machine as the Scheduler process. Other Executors, like the CeleryExecutor execute tasks using worker processes that exist on a separate cluster of worker machines. 
Workers: These are the processes that actually execute the logic of tasks, and are determined by the Executor being used.
