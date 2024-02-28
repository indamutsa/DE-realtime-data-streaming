import logging
import sys

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Keyspace in Cassandra is a namespace that defines data replication on nodes, it is a container for tables same as SQL database
def create_keyspace(session): # We create a keyspace in Cassandra to store the data
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session): # We create a table in the keyspace to store the data by executing a query
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")

def insert_data(session, **kwargs): # We are inserting data into the table by executing a query. This is not efficient for streaming data since it is a one time insert 
    print("inserting data...")

    # Extracting the data from the kwargs
    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try: # Executing the query to insert the data into the table
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')

def create_selection_df_from_kafka(spark_df): # We create a dataframe from the kafka stream. This is more efficient for streaming data than inserting one by one
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    # Selecting the data from the kafka stream
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

# We are connecting to kafka with the spark connection
def ccnnect_to_kafka(spark_connection):
    try:
        # Correcting 'readSream' to 'readStream'
        spark_dataframe = spark_connection.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()

        if spark_dataframe is None:
            logging.error("No DataFrame created from Kafka. Exiting...")
            # sys.exit(1)  # Exit or handle the error appropriately

        logging.info("Kafka dataframe created successfully")
        return spark_dataframe
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created due to {e}")
        # sys.exit(1)


def create_spark_connection():  # We create a spark session to connect to the spark cluster
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                                           "org.apache.commons:commons-pool2:2.8.0,"
                                           "org.apache.kafka:kafka-clients:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview2,"
                                           "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.0.0-preview2") \
            .config('spark.cassandra.connection.host', 'cassandra_db') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        print('Spark connection created successfully!')
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


# We create a connection to the Cassandra cluster
def create_cassandra_connection():
    try:

        # Connecting to cassandra cluster
        cluster = Cluster(['cassandra_db']) #locahost if you are checking from the terminal
        cass_session = cluster.connect('')
        return cass_session
    except Exception as e:
        logging.error(f"Could not create a connection to Cassandra due to exception {e}")
        return None

# Main logic, we make sure this code runs only this file is run as the entrypoint in a module
if __name__ == "__main__":
    # Establishing connections
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to kafka with spark connection
        spark_dataframe = ccnnect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_dataframe)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session) rather than insert once, we need to insert the stream...

            logging.info(f"Streaming is getting started...")

            # Writing stream to Cassandra using python spark_stream in the terminal
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option("checkpointLocation", "/tmp/checkpoint")
                               .option("keyspace", "spark_streams")
                               .option("table", "created_users")
                               .start())

            streaming_query.awaitTermination()
            # We can use the terminal to initiate the readstream to be written in cassandra
            # spark-submit --master spark://localhost:7077 \
            # --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
            # spark_stream.py
    else:
        logging.error("Failed to create DataFrame from Kafka. Exiting...")
else:
    logging.error("Spark session could not be established. Exiting...")