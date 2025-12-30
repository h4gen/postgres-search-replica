#!/bin/sh
set -e

# Start Postgres only if requested (default for sink)
if [ "$START_POSTGRES" = "true" ]; then
    echo "Starting local Postgres..."
    docker-entrypoint.sh postgres &

    # Wait for Postgres to be ready
    echo "Waiting for local Postgres to start..."
    until pg_isready -h localhost -p 5432 -U postgres; do
      sleep 1
    done
    echo "Local Postgres is ready."
fi

# If arguments are provided, run them
if [ $# -gt 0 ]; then
    echo "Running custom command: $@"
    exec "$@"
fi

# Otherwise run the replicator daemon
echo "Starting replicator daemon..."
exec /uv/bin/uv run python -m src.main
