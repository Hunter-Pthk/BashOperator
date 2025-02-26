from datetime import timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator

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
    dag_id='ETL_toll_data',
    default_args = default_args,
    description='Apache Airflow Final Assignment',
    schedule = timedelta(days=1),
    )

# define the tasks

# Unzip task
unzip = BashOperator(
    task_id='unzip',
    bash_command='tar -xzvf /opt/airflow/dags/finalassignment/tolldata.tgz -C /opt/airflow/dags/finalassignment/staging',
    dag=dag,
)

# extract task
extract_data_from_csv = BashOperator(
    task_id='extract',
    bash_command="cut -d ',' -f 1,2,3,4 /opt/airflow/dags/finalassignment/staging/vehicle-data.csv > /opt/airflow/dags/output/csv_data.csv",
    dag=dag,
)

unzip >> extract_data_from_csv