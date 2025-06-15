from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

import os
os.environ['HADOOP_HOME'] = r"C:\hadoop-3.3.6"
os.environ['PATH'] += r";C:\hadoop-3.3.6\bin"

spark = SparkSession.builder \
    .appName("KafkaSimpleConsumerTest") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.home.dir", r"C:\hadoop-3.2.2") \
    .getOrCreate()

# Схема повідомлення з Kafka
schema = StructType() \
    .add("id", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("timestamp", StringType())

# Зчитування потоку з Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "building_sensors") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Вивід у консоль
query = df_parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()