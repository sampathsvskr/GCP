from datetime import timedelta
from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
@dag(
    default_args=default_args,
    schedule_interval=None, 
    start_date=days_ago(1),
)
def example_dag1():
    def extract(multiple_outputs=True):
        return ["a","b","c"]

    @task(task_id="transform_data",retries=2)
    def transform(data) -> dict:
        return {'no_records': len(data)}

    @task
    def load(no_records):
        return f"no. of records = {no_records}"

    @task_group(group_id='group_1')
    def group_1(list):

        @task(task_id='subtask_1')
        def task_4(value):
            return sum(value)

        @task(task_id='subtask_2')
        def task_5(value):
            return int(value)*2

        # task_4 will sum the values of the list sent by group_1
        # task_5 will multiply it by two.
        task_5_result = task_5(task_4(list))

        return task_5_result

    load(transform(extract())["no_records"]) >> group_1([1,2,3])

dag = example_dag1()