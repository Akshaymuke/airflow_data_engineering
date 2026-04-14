from airflow.sdk import dag, task

@dag(
    dag_id="versioned_dag"
)
def versioned_dag():

    @task.python
    def first_task():
        print("this is first to execute")
    
    @task.python
    def second_task():
        print("this is second to execute")

    @task.python
    def third_task():
        print("this is third to execute")

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


versioned_dag()