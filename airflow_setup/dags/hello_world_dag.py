from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def task1_func():
    print("Task 1: 模拟拉取数据")

def task2_func():
    print("Task 2: 模拟处理数据")

def task3_func():
    print("Task 3: 模拟写入数据库")

with DAG(
    dag_id="testing_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="extract", python_callable=task1_func)
    t2 = PythonOperator(task_id="transform", python_callable=task2_func)
    t3 = PythonOperator(task_id="load", python_callable=task3_func)

    t1 >> t2 >> t3