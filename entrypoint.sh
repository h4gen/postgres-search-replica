#!/bin/sh
# Entrypoint for pg-search-replica monolithic service

# Ensure we use the right PYTHONPATH for our package
export PYTHONPATH=$PYTHONPATH:/app/src

# Start Postgres in background (using original entrypoint)
/usr/local/bin/docker-entrypoint.sh postgres &

# Wait for Postgres to be ready
echo "Waiting for Postgres..."
for i in {1..30}; do
    if pg_isready -h localhost -U postgres; then
        echo "Postgres is ready!"
        break
    fi
    sleep 1
done

# If a custom command is provided, run it
if [ $# -gt 0 ]; then
    echo "Running custom command: $@"
    exec "$@"
fi

# Otherwise, start the PGSearchReplica service in a restart loop
echo "Starting PGSearchReplica service..."
while true; do
    /uv/bin/uv run python -m pg_replica.cli start
    
    EXIT_CODE=$?
    
    # If exit code is 0, it was likely a clean shutdown (SIGTERM/SIGINT)
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Service exited normally."
        break
    fi
    
    echo "Service crashed (exit code $EXIT_CODE). Restarting in 5 seconds..."
    sleep 5
done
