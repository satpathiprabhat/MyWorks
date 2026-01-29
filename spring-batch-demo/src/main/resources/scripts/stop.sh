#!/bin/bash

APP_HOME=/opt/aadhaar-migration
PID_FILE=$APP_HOME/migration.pid
MAX_WAIT_SECONDS=120   # configurable
SLEEP_INTERVAL=5

if [ ! -f "$PID_FILE" ]; then
  echo "No PID file found. Migration may not be running."
  exit 1
fi

PID=$(cat $PID_FILE)

if ! ps -p $PID > /dev/null 2>&1; then
  echo "Process not running. Cleaning up PID file."
  rm -f $PID_FILE
  exit 0
fi

echo "[$(date)] Sending SIGTERM to migration process (PID=$PID)"
kill -15 $PID

echo "[$(date)] Waiting for graceful shutdown (max ${MAX_WAIT_SECONDS}s)..."

WAITED=0
while ps -p $PID > /dev/null 2>&1; do
  sleep $SLEEP_INTERVAL
  WAITED=$((WAITED + SLEEP_INTERVAL))

  if [ $WAITED -ge $MAX_WAIT_SECONDS ]; then
    echo "[$(date)] WARNING: Process did not stop within ${MAX_WAIT_SECONDS}s"
    echo "[$(date)] Manual intervention may be required."
    break
  fi
done

# Final verification
if ps -p $PID > /dev/null 2>&1; then
  echo "[$(date)] ERROR: Migration process still running (PID=$PID)"
  echo "[$(date)] NOT removing PID file."
  exit 2
else
  echo "[$(date)] Migration stopped successfully."
  rm -f $PID_FILE
  exit 0
fi