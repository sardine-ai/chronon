#!/bin/bash

set -e

# Function to handle SIGTERM
_term() {
  echo "Received SIGTERM signal, shutting down..."
  kill -TERM "$child" 2>/dev/null
}

if [ -z "$K8S_CPU_LIMIT_COUNT_X2" ]; then
  echo "K8S_CPU_LIMIT_COUNT_X2 is not set. Please set to CPU limit * 2"
  exit 1
fi

echo "Setting proccesor count to $K8S_CPU_LIMIT_COUNT_X2"

trap _term SIGTERM SIGINT

#https://pretius.com/blog/jvm-kubernetes/
java -XX:MaxRAMPercentage=75 -XX:+UseParallelGC -XX:ActiveProcessorCount=$K8S_CPU_LIMIT_COUNT_X2 -jar service.jar run ai.chronon.service.WebServiceVerticle -Dserver.port=9000 -conf /app/config.json &
child=$!  # Capture the Java process PID

# Wait for the Java process to exit
wait "$child"
