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

# extract from csv task
extract_data_from_csv = BashOperator(
    task_id='extract_from_csv',
    bash_command="cut -d ',' -f 1,2,3,4 /opt/airflow/dags/finalassignment/staging/vehicle-data.csv > /opt/airflow/dags/output/csv_data.csv",
    dag=dag,
)

# extract from tsv file task
extract_data_from_tsv = BashOperator(
    task_id='extract_from_tsv',
    bash_command="cut -f 5,6,7 /opt/airflow/dags/finalassignment/staging/tollplaza-data.tsv > /opt/airflow/dags/output/tsv_data.csv",
    dag=dag,
)

# extract from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_from_fixed_width',
    bash_command="awk '{print $10, $11}' /opt/airflow/dags/finalassignment/staging/payment-data.txt > /opt/airflow/dags/output/fixed_width_data.csv",
    dag=dag,
)

# combining into single csv file
consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command="paste -d, /opt/airflow/dags/output/csv_data.csv /opt/airflow/dags/output/tsv_data.csv /opt/airflow/dags/output/fixed_width_data.csv > /opt/airflow/dags/output/extracted_data.csv",
    dag=dag,
)

# Transformation of data
transform_data = BashOperator(
    task_id="transform_data",
    bash_command="""  
    awk -F',' 'NR==0 {print $0; next} {split($0, fields, ","); fields[4] = toupper(fields[4]); print fields[1] "," fields[2] "," fields[3] "," fields[4] "," fields[5]}' /opt/airflow/dags/output/extracted_data.csv > /opt/airflow/dags/finalassignment/staging/transformed_data.csv
    """,
    dag=dag,
)

unzip >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data