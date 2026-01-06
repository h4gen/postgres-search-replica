#!/bin/bash
set -m # Enable job control

# Ensure we use the right PYTHONPATH
export PYTHONPATH=$PYTHONPATH:/app/src

# Signal handler for graceful shutdown
_term() {
  echo "Caught SIGTERM/SIGINT signal!"
  kill -TERM "$DAEMON_PID" 2>/dev/null
  kill -TERM "$WEBAPP_PID" 2>/dev/null
  exit 0
}

trap _term SIGTERM SIGINT

# 1. Start Webapp (Background)
if [ -d "/app/webapp" ]; then
    echo "Starting Next.js Workbench UI..."
    cd /app/webapp && npm start &
    WEBAPP_PID=$!
    cd /app
else
    echo "Warning: /app/webapp not found, skipping UI start."
fi

# 2. Start PGSearchReplica (Background)
echo "Starting PGSearchReplica service via main.py..."
while true; do
  /uv/bin/uv run python -m pg_replica.main &
  DAEMON_PID=$!
  
  # Wait for daemon to exit
  wait "$DAEMON_PID"
  EXIT_CODE=$?

  if [ $EXIT_CODE -eq 0 ]; then
    echo "Daemon exited normally."
    kill -TERM "$WEBAPP_PID" 2>/dev/null
    break
  fi

  echo "Daemon crashed (exit code $EXIT_CODE). Restarting in 5 seconds..."
  sleep 5
done

# Wait for any remaining background jobs
wait
