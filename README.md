# Spotify ETL & Analytics Dashboard

A comprehensive data pipeline and analytics platform for Spotify listening data, featuring automated ETL processes, data transformation with dbt, and an interactive Streamlit dashboard.

## Features

- **ETL Pipeline**: Fetches data from Spotify API and stores it in PostgreSQL
- **Data Transformation**: Uses dbt for data modeling and analytics-ready views
- **Interactive Dashboard**: Real-time analytics dashboard built with Streamlit
- **Orchestration**: Apache Airflow for workflow management and scheduling
- **Containerized**: Fully dockerized setup for easy deployment

## Prerequisites

- Docker and Docker Compose
- Spotify Developer Account
- Python 3.8+

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/spotify-etl.git
cd spotify-etl
```

### 2. Set Up Spotify API Credentials

1. Go to [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
2. Create a new app
3. Note your Client ID and Client Secret
4. Add `http://localhost:8888/callback` as a redirect URI in your app settings

### 3. Configure Environment Variables

Create a `.env` file in the project root (reference `.env example` for template):

```bash
# Spotify API Configuration
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
SPOTIFY_REDIRECT_URI=http://localhost:8888/callback
SPOTIFY_SCOPE=user-read-recently-played user-top-read playlist-read-private

# PostgreSQL Configuration
POSTGRES_HOST=postgres
POSTGRES_USER=admin
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=airflow
POSTGRES_PORT=5432

# Spotify Database Configuration
SPOTIFY_DB=spotify
SPOTIFY_DB_HOST=postgres
SPOTIFY_DB_PORT=5432
SPOTIFY_DB_USER=analyst
SPOTIFY_DB_PASSWORD=analyst_password

# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_GID=0
```

### 4. Start the Services

```bash
# Start all services
docker-compose up -d

# Wait for services to initialize (about 30-60 seconds)
docker-compose ps  # Check status
```

### 5. Run Initial Data Collection

```bash
# First-time authentication (opens browser for Spotify login)
python src/main.py

# The script will:
# 1. Authenticate with Spotify
# 2. Fetch your recent listening history
# 3. Store data in PostgreSQL
```

### 6. Access the Dashboard

```bash
# Install dashboard dependencies
pip install -r dashboard/requirements.txt

# Run the Streamlit dashboard
streamlit run dashboard/main.py
```

Open your browser to `http://localhost:8501` to view the dashboard.

## Project Structure

```
spotify-etl/
├── src/                      # Source code
│   ├── main.py              # Main ETL script
│   ├── postgres_connection.py
│   └── spotify_dbt/         # dbt models
│       ├── models/
│       │   ├── staging/     # Raw data transformations
│       │   └── marts/       # Analytics models
│       └── dbt_project.yml
├── dashboard/               # Streamlit dashboard
│   ├── main.py             # Dashboard entry point
│   └── utils/              # Helper modules
│       ├── database.py
│       ├── data_loader.py
│       └── charts.py
├── dags/                   # Airflow DAGs
│   └── spotify_etl_dag.py
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Usage Guide

### Running the ETL Pipeline

#### Manual Execution
```bash
# Run ETL to fetch latest data
python src/main.py
```

#### Scheduled Execution (via Airflow)
1. Access Airflow UI: `http://localhost:8080`
2. Default credentials: `airflow` / `airflow`
3. Enable the `spotify_etl_pipeline` DAG
4. The pipeline will run daily at midnight (configurable)

### Using the Dashboard

The dashboard provides several views:

1. **Overview**: Key metrics, daily activity, and recent tracks
2. **Artists & Genres**: Top artists, genre preferences, and discovery analysis
3. **Listening Patterns**: Hourly and weekly listening habits
4. **Playlists**: Diversity analysis and composition metrics

#### Dashboard Features
- Time range selection (7, 30, 90 days, or all time)
- Real-time data refresh
- Interactive charts with hover details
- Export capabilities for reports

### Running dbt Transformations

```bash
# Navigate to dbt directory
cd src/spotify_dbt

# Install dbt dependencies
dbt deps

# Run all models
dbt run

# Run specific models
dbt run --select staging
dbt run --select marts

# Generate documentation
dbt docs generate
dbt docs serve
```

## Data Model

### Core Tables

- `user_listening_history`: Track play events with timestamps
- `tracks`: Track metadata and audio features
- `artists`: Artist information and genres
- `albums`: Album details
- `playlists`: User playlists
- `playlist_tracks`: Playlist-track relationships

### Analytics Views (via dbt)

- `user_listening_behavior`: Aggregated listening metrics
- `artist_and_genre_preferences`: Affinity scores and rankings
- `playlist_composition_metrics`: Diversity and composition analysis
- `listening_activity_report`: Time-based activity summaries

## Troubleshooting

### Database Connection Issues

```bash
# Check if PostgreSQL is running
docker-compose ps postgres

# Test connection
docker exec -it spotify-etl-postgres-1 psql -U admin -d spotify

# View logs
docker-compose logs postgres
```

### Spotify API Authentication

If authentication fails:
1. Verify your Client ID and Secret in `.env`
2. Check redirect URI matches exactly: `http://localhost:8888/callback`
3. Clear browser cache and try again
4. Regenerate credentials if needed

### Dashboard Not Loading Data

```bash
# Check database has data
docker exec -it spotify-etl-postgres-1 psql -U analyst -d spotify -c "SELECT COUNT(*) FROM user_listening_history;"

# Verify environment variables
python -c "import os; print(os.getenv('SPOTIFY_DB_HOST'))"

# Test connection
python dashboard/test_connection.py
```

### Airflow Issues

```bash
# Reset Airflow database
docker-compose down -v
docker-compose up -d

# Check Airflow logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler
```

## Advanced Configuration

### Adjusting Data Refresh Frequency

Edit `dags/spotify_etl_dag.py`:
```python
schedule_interval='@daily'  # Change to '@hourly' or custom cron
```

### Extending Data Collection

Modify `SPOTIFY_SCOPE` in `.env` to include additional permissions:
- `user-library-read`: Access saved tracks
- `user-follow-read`: Access followed artists
- `streaming`: Access real-time playback

### Custom Analytics

Add new dbt models in `src/spotify_dbt/models/`:
1. Create SQL file in appropriate directory
2. Add to `schema.yml`
3. Run `dbt run --select your_new_model`

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Spotify Web API for data access
- Apache Airflow for orchestration
- dbt for data transformation
- Streamlit for dashboard framework
- PostgreSQL for data storage