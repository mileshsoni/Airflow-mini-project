from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
import random
import math

# ---------------------------
# Default arguments for all tasks
# ---------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Tasks don't depend on previous DAG run
    'start_date': datetime(2025, 1, 1),  # When the DAG will start running
    'retries': 1,  # Number of retries in case of failure
    'retry_delay': timedelta(minutes=1),  # Delay between retries
}


# ---------------------------
# Task 1: Generate random numbers
# ---------------------------
def generate_random_numbers(**kwargs):
    """Generate 10 random numbers between 1-100"""
    numbers = [random.randint(1, 100) for _ in range(10)]
    print(f"Generated numbers: {numbers}")
    
    # Push numbers to XCom for other tasks to consume
    kwargs['ti'].xcom_push(key='random_numbers', value=numbers)
    return numbers

# ---------------------------
# Task 2: Calculate statistics (mean, median, std)
# ---------------------------
def calculate_stats(**kwargs):
    """Calculate basic statistics (mean, median, std deviation)"""
    ti = kwargs['ti']
    
    # Pull the generated numbers from XCom
    numbers = ti.xcom_pull(task_ids='generate_numbers', key='random_numbers')
    
    # Compute stats
    mean = sum(numbers) / len(numbers)
    median = sorted(numbers)[len(numbers)//2]
    std_dev = math.sqrt(sum((x - mean)**2 for x in numbers) / len(numbers))
    
    # Log the results
    print(f"Numbers: {numbers}")
    print(f"Mean: {mean:.2f}")
    print(f"Median: {median}")
    print(f"Standard Deviation: {std_dev:.2f}")
    
    # Push stats to XCom for the final report
    ti.xcom_push(key='stats', value={
        'mean': mean,
        'median': median,
        'std_dev': std_dev
    })

# ---------------------------
# Task 3: Filter even numbers
# ---------------------------
def filter_evens(**kwargs):
    """Filter out even numbers from the generated list"""
    ti = kwargs['ti']
    
    numbers = ti.xcom_pull(task_ids='generate_numbers', key='random_numbers')
    evens = [x for x in numbers if x % 2 == 0]
    
    print(f"Even numbers: {evens}")
    
    # Push even numbers to XCom
    ti.xcom_push(key='even_numbers', value=evens)

# ---------------------------
# Task 4: Square all numbers
# ---------------------------
def square_numbers(**kwargs):
    """Square all numbers in the list"""
    ti = kwargs['ti']
    
    numbers = ti.xcom_pull(task_ids='generate_numbers', key='random_numbers')
    squared = [x**2 for x in numbers]
    
    print(f"Squared numbers: {squared}")
    
    # Push squared numbers to XCom
    ti.xcom_push(key='squared_numbers', value=squared)

# ---------------------------
# Task 5: Generate final report
# ---------------------------
def final_report(**kwargs):
    """Compile all outputs and print final report"""
    ti = kwargs['ti']
    
    stats = ti.xcom_pull(task_ids='calculate_stats', key='stats')
    evens = ti.xcom_pull(task_ids='filter_evens', key='even_numbers')
    squared = ti.xcom_pull(task_ids='square_numbers', key='squared_numbers')
    
    # Log the final report
    print("\n=== FINAL REPORT ===")
    print(f"Statistical Analysis: {stats}")
    print(f"Even Numbers Found: {evens}")
    print(f"Squared Values: {squared}")
    print("=== END REPORT ===")


# ---------------------------
# Define the DAG and tasks
# ---------------------------
with DAG(
    'calculation_pipeline',
    default_args=default_args,
    description='A pipeline demonstrating calculations and data passing',
    schedule_interval=timedelta(minutes=30),  # Run every 30 minutes
    catchup=False,  # Don’t backfill for past dates
    tags=['demo'],
) as dag:

    # Task to generate random numbers
    generate_numbers = PythonOperator(
        task_id='generate_numbers',
        python_callable=generate_random_numbers,
    )

    # Task to calculate mean, median, std
    calc_stats = PythonOperator(
        task_id='calculate_stats',
        python_callable=calculate_stats,
    )

    # Task to filter even numbers
    filter_even = PythonOperator(
        task_id='filter_evens',
        python_callable=filter_evens,
    )

    # Task to square all numbers
    square_all = PythonOperator(
        task_id='square_numbers',
        python_callable=square_numbers,
    )

    # Task to print final report using XComs from previous tasks
    report = PythonOperator(
        task_id='final_report',
        python_callable=final_report,
    )

    # ---------------------------
    # Set up task dependencies
    # ---------------------------
    # First generate the numbers
    # Then calculate stats, filter evens, and square them in parallel
    # Finally compile everything into a report
    generate_numbers >> [calc_stats, filter_even, square_all]
    [calc_stats, filter_even, square_all] >> report
    
    
    