#!/bin/sh
# We don't use set -e to allow the restart loop to manage the daemon
# set -e

# 1. Start Postgres in background if requested
if [ "$START_POSTGRES" = "true" ]; then
    echo "Starting local Postgres..."
    # The standard postgres entrypoint handles initialization
    docker-entrypoint.sh postgres &
    
    echo "Waiting for local Postgres to start..."
    until pg_isready -h localhost -p 5432 -U postgres; do
      sleep 1
    done
    echo "Local Postgres is ready."
fi

# 2. Start pgai worker in background if requested
if [ "$START_PGAI_WORKER" = "true" ]; then
    echo "Starting pgai worker (pointing to localhost)..."
    # Hardcoded to localhost as it's part of the same unit
    /uv/bin/uv run pgai vectorizer worker \
        --db-url "${SINK_URL:-postgresql://postgres:password@localhost:5432/search_replica_db}" \
        --poll-interval 2s &
    WORKER_PID=$!
fi

# 3. Run the replicator daemon in a restart loop (Failsafe)
echo "Starting replicator daemon..."
while true; do
    # Run custom command if provided, otherwise run the replicator daemon
    if [ $# -gt 0 ]; then
        echo "Running custom command: $@"
        "$@"
    else
        /uv/bin/uv run python -m src.main
    fi
    
    EXIT_CODE=$?
    
    # If exit code is 0, it was likely a clean shutdown (SIGTERM/SIGINT)
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Process exited normally."
        break
    fi
    
    echo "Process crashed (exit code $EXIT_CODE). Restarting in 5 seconds..."
    sleep 5
done

# Cleanup: if the daemon loop exits, try to stop background processes
if [ ! -z "$WORKER_PID" ]; then
    kill $WORKER_PID 2>/dev/null
fi
