from airflow.hooks.postgres_hook import PostgresHook

def copy_to_db():
    """
    Copies data from a CSV file to a PostgreSQL database.
    """
    # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    
    # Define the path to the CSV file
    csv_file_path = '/Users/ducvu/Projects/spotify-etl-/src/recently_played.csv'
    
    # Define the SQL copy command
    sql_copy_command = f"""
        COPY recently_played 
        FROM '{csv_file_path}'
        DELIMITER ','
        CSV HEADER;
    """
    
    # Execute the copy command
    pg_hook.run(sql_copy_command)