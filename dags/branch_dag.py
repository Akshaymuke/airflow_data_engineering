from airflow.sdk import dag, task

@dag(
    dag_id="branch_dag"
)
def branch_dag():

    @task.python
    def extract_task(**kwargs):
        print("Extracting data.....")
        ti = kwargs['ti']
        extracted_data_dict = {
            "api_extracted_data":[1,2,3],
            "db_extracted_data":[4,5,6],
            "s3_extracted_data":[7,8,9],
            "weekend_flag": False
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
    
    @task.branch
    def decider_task(**kwargs):
        ti = kwargs['ti']
        weekend_flag = ti.xcom_pull(task_ids='extract_task')['weekend_flag']
        if weekend_flag is True:
            return 'no_load_task'
        else:
            return 'load_task'

    @task.bash
    def no_load_task():
        print("No loading on weekends....")
        return "echo 'No load task executed'"
    
    extract = extract_task()
    api_task = transform_task_api()
    db_task = transform_task_db()
    s3_task = transform_task_s3()
    decider = decider_task()
    load = load_task()
    no_load = no_load_task()

    extract >> [api_task, db_task, s3_task] >> decider >> [load,no_load]


branch_dag()