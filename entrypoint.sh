#!/bin/sh
set -e

# Start Postgres in the background
docker-entrypoint.sh postgres &

# Wait for Postgres to be ready
echo "Waiting for local Postgres to start..."
until pg_isready -h localhost -p 5432 -U postgres; do
  sleep 1
done
echo "Local Postgres is ready."

# Run the replicator daemon using uv run as a module
echo "Starting replicator daemon..."
exec /uv/bin/uv run python -m src.main
