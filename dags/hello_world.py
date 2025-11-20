from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


def print_hello():
    print("Hello, World!")


with DAG("hello_world", start_date=datetime(2025, 1, 1), schedule="@daily") as dag:
    hello_world = PythonOperator(task_id="hello_world", python_callable=print_hello)

    bash_hello_world = BashOperator(
        task_id="bash_hello_world", bash_command="echo 'Hello, World!'"
    )
    start = BashOperator(task_id="start", bash_command="echo 'Starting...'")
    end = BashOperator(task_id="end", bash_command="echo 'Ending...'")
    start >> hello_world >> bash_hello_world >> end
