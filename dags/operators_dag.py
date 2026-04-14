from airflow.sdk import dag, task
from airflow.operators import bash

@dag(
    dag_id="operators_dag"
)
def operators_dag():

    @task.python
    def first_task():
        print("this is first to execute")
    
    @task.python
    def second_task():
        print("this is second to execute")

    @task.bash
    def bash_task():
        return "echo this is bash task to execute."

    first = first_task()
    second = second_task()
    third = bash_task()

    first >> second >> third


operators_dag()