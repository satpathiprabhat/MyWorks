#!/bin/bash

APP_HOME=/opt/aadhaar-migration
CONFIG_DIR=$APP_HOME/config
JAR_NAME=aadhaar-hsm-migration.jar
LOG_DIR=$APP_HOME/logs
DATE=$(date +"%Y%m%d_%H%M%S")

mkdir -p $LOG_DIR

if [ -f "$APP_HOME/migration.pid" ]; then
  echo "Migration already running. Exiting."
  exit 1
fi

echo "[$(date)] Starting Aadhaar migration job"

java -Xms2g -Xmx4g \
  -jar $APP_HOME/$JAR_NAME \
  --spring.config.additional-location=file:$CONFIG_DIR/ \
  --spring.batch.job.names=aadhaarMigrationJob \
  --run.id=$DATE \
  > $LOG_DIR/migration_$DATE.log 2>&1 &

PID=$!
echo $PID > $APP_HOME/migration.pid

echo "[$(date)] Migration started with PID=$PID"