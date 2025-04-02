from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_postgres_connection',
    default_args=default_args,
    description='Test PostgreSQL connection',
    schedule_interval=None,
    start_date=datetime(2023, 10, 1),
    catchup=False,
)

test_postgres_task = PostgresOperator(
    task_id='test_postgres_connection',
    postgres_conn_id='postgres',
    sql="""
        SELECT 1;
    """,
    autocommit=True,
    dag=dag,
)


test_postgres_task