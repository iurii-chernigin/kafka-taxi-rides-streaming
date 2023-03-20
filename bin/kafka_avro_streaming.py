#!/usr/bin/env python
# coding: utf-8

# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-avro_2.12:3.3.1 pyspark-shell'

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.sql.avro.functions import from_avro

from settings import KAFKA_BOOTSTRAP_SERVER, SCHEMA_GREEN_RIDE, SCHEMA_REGISTRY_ADDRESS, SCHEMA_REGISTRY_KEY, SCHEMA_REGISTRY_SECRET, SCHEMA_REGISTRY_SECRET, SCHEMA_REGISTRY_OPTIONS, KAFKA_CLUSTER_KEY, KAFKA_CLUSTER_SECRET



def read_from_kafka(topic: str) -> DataFrame:
    """
    Subscribe to Kafka topic and create streaming DataFame  
    :topic a topic with source data 
    """
    return spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)\
        .option("kafka.security.protocol", "SASL_SSL")\
        .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(KAFKA_CLUSTER_KEY, KAFKA_CLUSTER_SECRET))\
        .option("kafka.sasl.mechanism", "PLAIN")\
        .option("kafka.ssl.endpoint.identification.algorithm", "https")\
        .option("subscribe", topic)\
        .option("startingOffset", "latest")\
        .option("checkpoingLocation", "checkpoint")\
        .load()


def parse_avro_events(df_stream: DataFrame) -> DataFrame:
    """
    Parse AVRO events from a subscribed topic using Confluent schema registry
    :df_stream stream DataFrame with subscribtion to a Kafka topic
    """
    assert df_stream.isStreaming is True, "DataFrame doesn't receive streaming data"
    return df_stream.select(
        from_avro(
            data = F.col("key"),
            options = SCHEMA_REGISTRY_OPTIONS,
            subject = "t-key",
            schemaRegistryAddress = SCHEMA_REGISTRY_ADDRESS
        ).alias("key"),
        from_avro(
            data = F.col("value"),
            options = SCHEMA_REGISTRY_OPTIONS,
            subject = "t-value",
            schemaRegistryAddress = SCHEMA_REGISTRY_ADDRESS
        ).alias("value"),
    )


def parse_json_events(df_stream: DataFame) -> DataFrame:
    """
    Parse JSON events from a subscribed topic using Confluent schema registry
    :df_stream stream DataFrame with subscribtion to a Kafka topic
    """
    df = df_stream.selectExpr("CAST(key as STRING)", "CAST(value as STRING)")
    col = F.split(df["key"], ",")

    for idx, field in enumerate(SCHEMA_GREEN_RIDE):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))

    return df.select([field.name for field in SCHEMA_GREEN_RIDE])


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Spark-Notebook").getOrCreate()

    df_stream = read_from_kafka("rides_green")
    print(df_stream.printSchema())
    df_rides = parse_avro_events(df_stream)


    df_rides.writeStream\
        .outputMode("append")\
        .format("console")\
        .trigger(processingTime="5 seconds")\
        .option("truncate", False)\
        .start()

    spark.streams.awaitAnyTermination()



