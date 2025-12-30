#!/bin/sh
# Entrypoint for pg-search-replica monolithic service

# Ensure we use the right PYTHONPATH for our package
export PYTHONPATH=$PYTHONPATH:/app/src

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
