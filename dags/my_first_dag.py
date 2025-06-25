from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------
# Default arguments applied to all tasks in the DAG
# ---------------------------
default_args = {
    'owner': 'airflow',                     # Owner name for reference
    'depends_on_past': False,               # Task doesn’t wait for the previous run to succeed
    'email_on_failure': False,              # Disable email alerts on failure
    'email_on_retry': False,                # Disable email alerts on retries
    'retries': 1,                           # Number of retry attempts for each task
    'retry_delay': timedelta(minutes=5),    # Delay between retries
}


# ---------------------------
# Define the DAG (Directed Acyclic Graph)
# ---------------------------

with DAG(
    'my_first_dag',                         # Unique identifier for the DAG
    default_args=default_args,              # Apply the default_args defined above
    description='A simple tutorial DAG',    # Description shown in the Airflow UI
    schedule_interval=timedelta(days=1),    # Run the DAG once every day
    start_date=datetime(2023, 1, 1),        # Start date for the DAG (backfill will be skipped due to catchup=False)
    catchup=False,                          # Don’t run missed DAG runs between start_date and today
    tags=['example'],                       # Optional tags for categorizing in the UI
) as dag:


    # Task 1: Print the current system date
    t1 = BashOperator(
        task_id='print_date',               # Task ID (unique within the DAG)
        bash_command='date',                # Bash command to execute
    )

    # Task 2: Sleep for 5 seconds
    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,              # This task doesn’t depend on past DAG runs
        bash_command='sleep 5',             # Bash command to execute
        retries=3,                          # Retry this task up to 3 times if it fails
    )

    # Set task dependencies (t1 must run before t2)
    t1 >> t2
    
    
    
    