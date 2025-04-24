from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025,1,1),
    'retries': 1,
    'retry_delay': timedelta(seconds=45)
}

dag = DAG(
    'log_cleanup',
    default_args=default_args,
    description='Clean up old log files',
    schedule_interval='@weekly',
    catchup=False,
)

# Find and remove log files older than 30 days
cleanup_task = BashOperator(
    task_id='cleanup_airflow_logs',
    bash_command='find /opt/airflow/logs -type f -name "*.log" -mtime +30 -delete',
    dag=dag,
)

dbt_log_cleanup = BashOperator(
    task_id='cleanup_dbt_logs',
    bash_command='find /opt/airflow/logs -name "dbt*.log" -mtime +15 -delete',
    dag=dag,
)