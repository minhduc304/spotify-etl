# 🎵 Spotify Analytics Dashboard

A Streamlit-based dashboard for visualizing Spotify listening data processed through the dbt pipeline.

## Features

- **Overview**: Daily listening metrics, completion rates, and recent activity
- **Artists & Genres**: Top artists by affinity, genre preferences, discovery analysis
- **Listening Patterns**: Hourly/weekly patterns, listening behavior metrics
- **Playlists**: Diversity analysis and composition metrics

## Setup

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Environment Setup**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

3. **Ensure Database is Running**
   ```bash
   # From project root
   docker-compose up postgres-spotify
   ```

4. **Run Dashboard**
   ```bash
   streamlit run main.py
   ```

## Prerequisites

- PostgreSQL database with Spotify data (from ETL process)
- dbt models successfully built
- Environment variables configured

## Architecture

```
dashboard/
├── main.py              # Main Streamlit application
├── utils/
│   ├── database.py      # Database connection handling
│   ├── data_loader.py   # Data loading with caching
│   └── charts.py        # Chart generation utilities
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

## Data Sources

The dashboard connects to the following dbt models:
- `public_analytics.artist_and_genre_preferences`
- `public_reporting.listening_activity_report`
- `public_reporting.playlist_diversity_report`
- Raw tables: `tracks`, `artists`, `user_listening_history`

## Troubleshooting

### Connection Issues
1. Verify PostgreSQL is running: `docker ps | grep postgres`
2. Check environment variables in `.env`
3. Test connection: `psql -h localhost -p 5433 -U analyst -d spotify`

### Data Issues
1. Ensure ETL process has been run
2. Verify dbt models are built: `dbt run`
3. Check data exists: `SELECT COUNT(*) FROM user_listening_history;`

### Performance
- Data is cached for 5 minutes using `@st.cache_data`
- Use the "Refresh Data" button to clear cache
- Limit time ranges for large datasets

## Customization

### Adding New Charts
1. Add data loading method to `utils/data_loader.py`
2. Create chart method in `utils/charts.py`
3. Add to appropriate tab in `main.py`

### Styling
- Colors and theme defined in `utils/charts.py`
- Custom CSS in `main.py`
- Spotify brand colors used throughout