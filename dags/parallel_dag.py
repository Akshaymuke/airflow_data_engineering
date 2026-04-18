from airflow.sdk import dag, task

@dag(
    dag_id="parallel_dag"
)
def parallel_dag():

    @task.python
    def extract_task(**kwargs):
        print("Extracting data.....")
        ti = kwargs['ti']
        extracted_data_dict = {
            "api_extracted_data":[1,2,3],
            "db_extracted_data":[4,5,6],
            "s3_extracted_data":[7,8,9]
        }
        ti.xcom_push(key="return_value", value=extracted_data_dict)
    
    @task.python
    def transform_task_api(**kwargs):
        print("Transforming data.....")
        ti = kwargs['ti']
        api_extracted_data = ti.xcom_pull(task_ids='extract_task')
        transformed_api_data = 2 * api_extracted_data['api_extracted_data']
        ti.xcom_push(key='return_value', value=transformed_api_data)

    @task.python
    def transform_task_db(**kwargs):
        print("Transforming data.....")
        ti = kwargs['ti']
        db_extracted_data = ti.xcom_pull(task_ids='extract_task')
        transformed_db_data = 2 * db_extracted_data['db_extracted_data']
        ti.xcom_push(key='return_value', value=transformed_db_data)

    @task.python
    def transform_task_s3(**kwargs):
        print("Transforming data.....")
        ti = kwargs['ti']
        s3_extracted_data = ti.xcom_pull(task_ids='extract_task')
        transformed_s3_data = 2 * s3_extracted_data['s3_extracted_data']
        ti.xcom_push(key='return_value', value=transformed_s3_data)
    
    @task.bash
    def load_task(**kwargs):
        print("Loading data to destination")
        api_data = kwargs['ti'].xcom_pull(task_ids='transformed_api_data')
        db_data = kwargs['ti'].xcom_pull(task_ids='transformed_db_data')
        s3_data = kwargs['ti'].xcom_pull(task_ids='transformed_s3_data')
        return f"echo 'Loaded data: {api_data}, {db_data}, {s3_data}'"
    
    extract_task = extract_task()
    api_task = transform_task_api()
    db_task = transform_task_db()
    s3_task = transform_task_s3()
    load_task = load_task()

    extract_task >> [api_task, db_task, s3_task] >> load_task


parallel_dag()