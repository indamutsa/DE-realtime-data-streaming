FROM apache/airflow:2.8.2

USER root

RUN apt-get update && \
    apt-get install -y gcc python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
