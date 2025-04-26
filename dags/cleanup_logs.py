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

# Find and remove log files older than specfied days
cleanup_task = BashOperator(
    task_id='cleanup_airflow_logs',
    bash_command='find ${LOGS_PATH} -type f -name "*.log" -mtime +30 -delete',
    dag=dag,
)

dbt_log_cleanup = BashOperator(
    task_id='cleanup_dbt_logs',
    bash_command='find $LOGS_PATH -type f -name "dbt*.log" -mtime +15 -delete',
    dag=dag,
)

scheduler_cleanup = BashOperator(
    task_id='cleanup_scheduler_logs',
    bash_command='find $LOGS_PATH -type d -name "scheduler" -mtime +30 -delete',
    dag=dag
)

processor_manager_cleanup = BashOperator(
    task_id='cleanup_processor_manager_logs',
    bash_command='find $LOGS_PATH -type d -name "dag_processor_manager" -mtime +30 -delete',
    dag=dag
)

dag_runs_cleanup = BashOperator(
    task_id='cleanup_dag_run_logs',
    bash_command='find $LOGS_PATH -type d -name "dag_id*" -mtime +30 -delete',
    dag=dag
)


