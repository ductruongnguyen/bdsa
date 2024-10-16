from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import subprocess
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Set start date for the DAG, adjust if necessary
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'scrapy_spider_schedule',  # Name of the DAG
    default_args=default_args,
    description='DAG to run Scrapy spiders in sequence',
    schedule_interval=None,  # You can set a schedule here, e.g., '0 12 * * *' for daily at noon
)


# Define Python functions that will call Scrapy spiders using subprocess

def run_spider(spider_name, project_path):
    """
    Run a Scrapy spider by name from the given project directory.
    :param spider_name: Name of the spider to run.
    :param project_path: Path to the Scrapy project where the spider is defined.
    """
    # Change the working directory to the Scrapy project path
    os.chdir(project_path)
    # Run the Scrapy spider using subprocess
    subprocess.run(['scrapy', 'crawl', spider_name])


# Define the Airflow tasks
executing_path = '/home/truongnd/bdsa/Data_Warehouse/data-ingestion/projects/tiki/tiki'

# Task 1: Run categories_crawler spider
categories_crawler_task = PythonOperator(
    task_id='run_categories_spider',
    python_callable=run_spider,
    op_kwargs={
        'spider_name': 'categories_crawler',  # Name of the spider
        'project_path': executing_path,  # Path to Scrapy project directory
    },
    dag=dag,
)

# Task 2: Run prd_list_crawler spider
prd_list_crawler_task = PythonOperator(
    task_id='run_prd_list_spider',
    python_callable=run_spider,
    op_kwargs={
        'spider_name': 'prd_list_crawler',  # Name of the spider
        'project_path': executing_path,  # Path to Scrapy project directory
    },
    dag=dag,
)

# Task 3: Run prd_detail_crawler spider
prd_detail_crawler_task = PythonOperator(
    task_id='run_prd_detail_spider',
    python_callable=run_spider,
    op_kwargs={
        'spider_name': 'prd_detail_crawler',  # Name of the spider
        'project_path': executing_path,  # Path to Scrapy project directory
    },
    dag=dag,
)

# Set task dependencies: categories_crawler -> prd_list_crawler -> prd_detail_crawler
categories_crawler_task >> prd_list_crawler_task >> prd_detail_crawler_task
