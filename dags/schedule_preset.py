from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    dag_id="first_schedule_dag_tryout",
    start_date = datetime(year=2026,month=1,day=1,tz='Asia/Calcutta'),
    schedule="@daily",
    is_paused_upon_creation=False
)
def first_schedule_dag_tryout():

    @task.python
    def first_task():
        print("this is first to execute")
    
    @task.python
    def second_task():
        print("this is second to execute")

    @task.python
    def third_task():
        print("this is third to execute")

    @task.python
    def fourth_task():
        print("this is last task to execute")

    first = first_task()
    second = second_task()
    third = third_task()
    fourth = fourth_task()

    first >> second >> third >> fourth


first_schedule_dag_tryout()