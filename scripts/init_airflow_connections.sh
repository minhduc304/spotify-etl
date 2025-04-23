#!/bin/bash
echo "Creating Airflow connections..."

airflow connections add 'spotify_db' \
    --conn-uri "Postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}"

echo "Connections created."
