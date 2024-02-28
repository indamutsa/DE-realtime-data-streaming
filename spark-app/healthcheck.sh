#!/bin/bash

# Check Cassandra readiness
if ! nc -z cassandra_db 9042; then
  echo "Cassandra is not ready"
  exit 1
fi

# Check Spark Master readiness
if ! nc -z spark-main 8080; then
  echo "Spark Master is not ready"
  exit 1
fi

echo "Both Cassandra and Spark Master are ready"
exit 0
