from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
)
from pyspark.sql.functions import from_json, col
import os


# directory where Spark will store its checkpoint data. crucial in streaming to enable fault tolerance
checkpoint_dir = "/tmp/checkpoint/kafka_to_postgres"
if not os.path.exists(checkpoint_dir):
    os.makedirs(checkpoint_dir)


# Configuration for connecting to the PostgreSQL database. This will be used to write the processed data from Spark to PostgreSQL.
postgres_config = {
    "url": "jdbc:postgresql://postgres:5432/stock_data",
    "user": "admin",
    "password": "admin",
    "dbtable": "stocks",
    "driver": "org.postgresql.Driver",
}


# The schema/structure matching the new data coming from Kafka
kafka_data_schema = StructType(
    [
        StructField("date", StringType()),
        StructField("high", StringType()),
        StructField("low", StringType()),
        StructField("open", StringType()),
        StructField("close", StringType()),
        StructField("symbol", StringType()),
    ]
)


spark = SparkSession.builder.appName("KafkaToPostgresStreaming").getOrCreate()


df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    #   .option('kafka.bootstrap.servers', 'localhost:9092')
    .option(
        "subscribe", "stock_analysis"
    )  # Subscribe to the Kafka topic 'stock_analysis'
    .option(
        "startingOffsets", "earliest"
    )  # Start from the earliest messages in the topic
    .option("failOnDataLoss", "false")  # Handle potential data loss gracefully
    .load()  # Read the streaming data from Kafka topic 'stock_analysis'
)


# Convert the 'value' column from Kafka (which is in binary format) to a string and then parse it as JSON using the defined schema
parsed_df = (
    df.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), kafka_data_schema).alias("data"))
    .select("data.*")
)  # Extract the fields from the parsed JSON

# Add this for debugging
# debug_df = df.selectExpr("CAST(value AS STRING)")
# debug_df.writeStream.format("console").start()


processed_df = parsed_df.select(
    col("date").cast(TimestampType()).alias("date"),
    col("high").alias("high"),
    col("low").alias("low"),
    col("open").alias("open"),
    col("close").alias("close"),
    col("symbol").alias("symbol"),
)


# Display the results in the console for debugging purposes. In production, this would be written to a database or another sink.
# query = (processed_df.writeStream
#          .outputMode("append")  # Append new data to the output
#          .format("console")  # Output to console for debugging
#          .option("truncate", "false")  # Do not truncate the output
#          .option("checkpointLocation", checkpoint_dir)  # Set the checkpoint location for fault tolerance
#          .start()  # Start the streaming query
# )


def write_to_postgres(batch_df, batch_id):
    """
    Write a micro-batch Dataframe to PostgreSQL using JDBC in 'append' mode. This function will be called for each batch of data processed by the streaming query.
    """
    batch_df.write.format("jdbc").mode("append").options(
        **postgres_config
    ).save()  # Save the batch of data to PostgreSQL


# -------------- Stream the processed data to PostgreSQL using foreachBatch------------------
query = (
    processed_df.writeStream.foreachBatch(
        write_to_postgres
    )  # Output to console for debugging
    .option(
        "checkpointLocation", checkpoint_dir
    )  # Set the checkpoint location for fault tolerance
    .outputMode("append")  # Append new data to the output
    .start()  # Start the streaming query
)


# SQL QUERY TO CREATE A TABLE§
# CREATE TABLE stocks (
#     date TIMESTAMP WITHOUT TIME ZONE,
#     high VARCHAR(10),
#     low VARCHAR(10),
#     open VARCHAR(10),
#     close VARCHAR(10),
#     symbol VARCHAR(10)
# )

# QUERY TO RUN IN THE STOCKS TABLE
# SELECT * FROM stocks

# QUERY TO DROP TABLE
# DROP TABLE stocks


query.awaitTermination()  # Keep the streaming query running until it is manually stopped
