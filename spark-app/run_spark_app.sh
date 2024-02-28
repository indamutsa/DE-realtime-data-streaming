#!/bin/bash

# Set PATH
export PATH=/usr/local/bin:/usr/bin:/bin

# Export the SPARK_SUBMIT_ARGS environment variable
export SPARK_SUBMIT_ARGS="--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --conf spark.cassandra.connection.host=cassandra_db"

# Run the Spark application
spark-submit --master "spark://spark-main:7077" $SPARK_SUBMIT_ARGS spark_stream.py
