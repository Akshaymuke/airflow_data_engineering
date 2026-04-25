from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
    dag_id="delta_schedule_dag",
    start_date = datetime(year=2026,month=4,day=15,tz='Asia/Calcutta'),
    schedule=DeltaTriggerTimetable(duration(days=3)),
    end_date= datetime(year=2026, month=4, day=27, tz="Asia/Calcutta"),
    is_paused_upon_creation=False,
    catchup=True
)
def delta_schedule_dag():

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


delta_schedule_dag()