#!/bin/bash
# setup_env.sh - Place this in your root-project-folder

# Get the absolute path to the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load variables from .env file
set -a
source "${PROJECT_ROOT}/.env"
set +a


echo "Environment variables loaded from ${PROJECT_ROOT}/.env"

