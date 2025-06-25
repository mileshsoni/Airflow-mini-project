import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__)))  # ✅ Good fallback

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from scripts import load_data, train_model, save_model  # ✅ Correct if scripts/ is in dags/

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='ml_pipeline_orchestration',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['mlops', 'airflow'],
) as dag:

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data.load_and_save,
    )

    train_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model.train,
    )

    save_task = PythonOperator(
        task_id='save_model',
        python_callable=save_model.save,
    )

    load_task >> train_task >> save_task
