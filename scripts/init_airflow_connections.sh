#!/bin/bash
echo "Creating Airflow connections..."


airflow connections add 'spotify_db' \
    --conn-uri "Postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres-airflow:5432/${POSTGRES_DB}"

airflow connections add 'spotify_postgres' \
    --conn-uri "postgresql://${SPOTIFY_DB_USER}:${SPOTIFY_DB_PASSWORD}@postgres-spotify:5432/${SPOTIFY_DB}"


echo "Connections created."
