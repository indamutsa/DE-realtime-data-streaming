#!/bin/bash
# This shebang line indicates the scripts should be run in the bash shell.

set -e # The `set -e` command causes the scripts to exit immediately if a command exits with a non-zero status.

# Check if the requirements.txt file exists at the specified path.
if [ -e /opt/airflow/requirements.txt ]; then
  # If the file exists, use pip to install Python packages listed in requirements.txt.
  # The `$(command -v pip)` command finds the path to the pip executable.
  $(command -v pip) install --user -r /opt/airflow/requirements.txt
fi

# Check if the airflow.db file does not exist in the specified directory.
if [ ! -f "/opt/airflow/airflow.db" ]; then
  # Initialize the Airflow database if airflow.db does not exist.
  airflow db init;
  # Create a default Airflow user with the specified properties.
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

# Upgrade the Airflow database to the latest version.
$(command -v airflow) db upgrade

# Start the Airflow webserver.
exec airflow webserver
