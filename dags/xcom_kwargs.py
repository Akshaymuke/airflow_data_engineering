from airflow.sdk import dag, task

@dag(
    dag_id="xcom_kwargs_dag"
)
def xcom_kwargs_dag():

    @task.python
    def first_task(**kwargs):
        ti = kwargs['ti']
        print("Extract data....first task")
        fetched_data = {"data":[1,2,3,4,5]}
        return fetched_data
    
    @task.python
    def second_task(data:dict):
        fetched_data = data["data"]
        transformed_data = fetched_data * 2
        transformed_data_dict = {"transformed_data":transformed_data}
        return transformed_data_dict
    
    @task.python
    def third_task(data:dict):
        return data

    first = first_task()
    second = second_task(first)
    third = third_task(second)


xcom_kwargs_dag ()