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

get_recently_played_task = PostgresOperator(
    task_id='get_recently_played',
    postgres_conn_id='postgres',
    sql="""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'recently_played'
        );
    """,
    autocommit=True,
    dag=dag,
)




get_recently_played_task