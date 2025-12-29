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

# Initialize the sink database if it doesn't exist (handled by docker-entrypoint typically)
# But we need to make sure our search_replica_db exists if not default
# The POSTGRES_DB env var in docker-compose handles this.

# Run the replicator daemon using uv run
echo "Starting replicator daemon..."
exec /uv/bin/uv run python src/replicator.py

