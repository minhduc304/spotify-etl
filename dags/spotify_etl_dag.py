from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
import sys

# Add src directory to Python path
sys.path.insert(0, '/opt/airflow/src')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spotify_etl_pipeline',
    default_args=default_args,
    description='Automated Spotify data extraction and transformation pipeline',
    schedule_interval='@daily',  # Run daily at midnight
    catchup=False,
    tags=['spotify', 'etl', 'dbt'],
)

def extract_spotify_data():
    """Execute the main Spotify ETL script"""
    from main import SpotifyETL

    try:
        # Initialize and run ETL
        etl = SpotifyETL()

        # Fetch recent listening history
        etl.fetch_recent_listening_history()

        # Fetch user playlists
        etl.fetch_user_playlists()

        # Fetch top artists and tracks
        etl.fetch_top_artists_and_tracks()

        print("Spotify data extraction completed successfully")
        return True

    except Exception as e:
        print(f"Error in Spotify ETL: {str(e)}")
        raise

# Task 1: Extract data from Spotify API
extract_task = PythonOperator(
    task_id='extract_spotify_data',
    python_callable=extract_spotify_data,
    dag=dag,
)

# Task 2: Data quality checks
with TaskGroup('data_quality_checks', dag=dag) as quality_checks:

    check_listening_history = PostgresOperator(
        task_id='check_listening_history',
        postgres_conn_id='postgres_spotify',
        sql="""
            SELECT
                COUNT(*) as record_count,
                MAX(played_at) as latest_play,
                COUNT(DISTINCT track_id) as unique_tracks
            FROM user_listening_history
            WHERE played_at >= CURRENT_DATE - INTERVAL '1 day';
        """,
        dag=dag,
    )

    check_tracks = PostgresOperator(
        task_id='check_tracks_integrity',
        postgres_conn_id='postgres_spotify',
        sql="""
            SELECT
                COUNT(*) as total_tracks,
                COUNT(CASE WHEN name IS NULL THEN 1 END) as missing_names,
                COUNT(CASE WHEN album_id IS NULL THEN 1 END) as missing_albums
            FROM tracks;
        """,
        dag=dag,
    )

    check_artists = PostgresOperator(
        task_id='check_artists_integrity',
        postgres_conn_id='postgres_spotify',
        sql="""
            SELECT
                COUNT(*) as total_artists,
                COUNT(CASE WHEN name IS NULL THEN 1 END) as missing_names,
                AVG(popularity) as avg_popularity
            FROM artists;
        """,
        dag=dag,
    )

# Task 3: Run dbt transformations
with TaskGroup('dbt_transformations', dag=dag) as dbt_tasks:

    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/src/spotify_dbt && dbt deps',
        dag=dag,
    )

    dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='cd /opt/airflow/src/spotify_dbt && dbt run --select staging',
        dag=dag,
    )

    dbt_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command='cd /opt/airflow/src/spotify_dbt && dbt run --select marts',
        dag=dag,
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/src/spotify_dbt && dbt test',
        dag=dag,
    )

    # Set dbt task dependencies
    dbt_deps >> dbt_staging >> dbt_marts >> dbt_test

# Task 4: Generate analytics summary
generate_summary = PostgresOperator(
    task_id='generate_analytics_summary',
    postgres_conn_id='postgres_spotify',
    sql="""
        -- Create or update daily summary
        INSERT INTO analytics_summary (
            summary_date,
            total_plays,
            unique_tracks,
            unique_artists,
            total_minutes,
            top_artist,
            top_track
        )
        SELECT
            CURRENT_DATE as summary_date,
            COUNT(*) as total_plays,
            COUNT(DISTINCT h.track_id) as unique_tracks,
            COUNT(DISTINCT ta.artist_id) as unique_artists,
            SUM(t.duration_ms::float / 60000) as total_minutes,
            (SELECT a.name FROM artists a
             JOIN track_artists ta2 ON a.artist_id = ta2.artist_id
             JOIN user_listening_history h2 ON ta2.track_id = h2.track_id
             WHERE h2.played_at >= CURRENT_DATE - INTERVAL '1 day'
             GROUP BY a.artist_id, a.name
             ORDER BY COUNT(*) DESC LIMIT 1) as top_artist,
            (SELECT t2.name FROM tracks t2
             JOIN user_listening_history h3 ON t2.track_id = h3.track_id
             WHERE h3.played_at >= CURRENT_DATE - INTERVAL '1 day'
             GROUP BY t2.track_id, t2.name
             ORDER BY COUNT(*) DESC LIMIT 1) as top_track
        FROM user_listening_history h
        JOIN tracks t ON h.track_id = t.track_id
        LEFT JOIN track_artists ta ON t.track_id = ta.track_id
        WHERE h.played_at >= CURRENT_DATE - INTERVAL '1 day'
        ON CONFLICT (summary_date)
        DO UPDATE SET
            total_plays = EXCLUDED.total_plays,
            unique_tracks = EXCLUDED.unique_tracks,
            unique_artists = EXCLUDED.unique_artists,
            total_minutes = EXCLUDED.total_minutes,
            top_artist = EXCLUDED.top_artist,
            top_track = EXCLUDED.top_track,
            updated_at = CURRENT_TIMESTAMP;

        -- First create the table if it doesn't exist
        CREATE TABLE IF NOT EXISTS analytics_summary (
            summary_date DATE PRIMARY KEY,
            total_plays INTEGER,
            unique_tracks INTEGER,
            unique_artists INTEGER,
            total_minutes NUMERIC,
            top_artist VARCHAR(255),
            top_track VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    dag=dag,
)

# Task 5: Clean up old data (optional, keeps last 90 days)
cleanup_old_data = PostgresOperator(
    task_id='cleanup_old_data',
    postgres_conn_id='postgres_spotify',
    sql="""
        -- Archive old listening history (older than 90 days)
        DELETE FROM user_listening_history
        WHERE played_at < CURRENT_DATE - INTERVAL '90 days'
        AND track_id IN (
            SELECT track_id FROM user_listening_history
            GROUP BY track_id
            HAVING COUNT(*) > 1
        );
    """,
    dag=dag,
    trigger_rule='all_done',  # Run even if previous tasks fail
)

# Task 6: Send notification (placeholder for email/slack notification)
def send_completion_notification(**context):
    """Send notification about pipeline completion"""
    execution_date = context['execution_date']
    print(f"Spotify ETL Pipeline completed successfully for {execution_date}")
    # Add email or Slack notification logic here if needed
    return True

notify_completion = PythonOperator(
    task_id='send_notification',
    python_callable=send_completion_notification,
    dag=dag,
    trigger_rule='all_done',
)

# Define task dependencies
extract_task >> quality_checks >> dbt_tasks >> generate_summary >> cleanup_old_data >> notify_completion