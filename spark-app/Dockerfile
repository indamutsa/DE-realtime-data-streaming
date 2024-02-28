# Use an official Python runtime as a parent image
FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y gcc python3-dev cron procps default-jdk netcat-openbsd vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* \

# Set the JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/default-java

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY . .
#COPY spark-cron /etc/cron.d/spark-cron

# Make the healthcheck script executable
RUN chmod +x healthcheck.sh
RUN chmod +x run_spark_app.sh

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable
ENV PYSPARK_PYTHON=python3

# Give execution rights on the cron job
#RUN chmod 0644 /etc/cron.d/spark-cron

# Apply cron job
#RUN crontab /etc/cron.d/spark-cron

# Run the command on container startup
#CMD cron -f

CMD ["./run_spark_app.sh"]

# ------------
# Run spark_stream.py when the container launches
#CMD ["spark-submit", "--master", "spark://spark-main:7077", "spark_stream.py"]

# Define environment variable for Python to use
#ENV PYSPARK_PYTHON=python3
#
## Run a shell command that loops indefinitely, running the Spark application every 2 minutes
#CMD while true; do \
#      spark-submit --master "spark://spark-main:7077" --conf "spark.ui.port=12999" spark_stream.py; \
#      sleep 120; \
#    done
