from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag = DAG(
    'test_postgres_connection',
    default_args=default_args,
    description='Test PostgreSQL connection',
    schedule_interval=None,
    start_date=datetime(2023, 10, 1),
    catchup=False,
)

get_user_listening_history = PostgresOperator(
    task_id='get_user_listening_history',
    postgres_conn_id='postgres',
    sql="""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'user_listening_history'
        );
    """,
    autocommit=True,
    dag=dag,
)

get_user_listening_history