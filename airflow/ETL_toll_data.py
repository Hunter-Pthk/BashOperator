from datetime import timedelta
from airflow import DAG

from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner' : 'Hello Bonjour',
    'start_date' : days_ago(0),
    'email' : ['example@gmail.com'],
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

# Define the DAG
dag=DAG(
    dag_id:'ETL_toll_data',
    default_args = default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval = timedelta(days=1),
)