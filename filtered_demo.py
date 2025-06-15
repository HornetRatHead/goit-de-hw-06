from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, DoubleType, StringType, TimestampType

spark = SparkSession.builder \
    .appName("FilteredDataDemo") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Схема даних
sensor_schema = StructType() \
    .add("id", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("timestamp", StringType())

# Читаємо з Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "building_sensors") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), sensor_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Фільтруємо: температура > 30
filtered_df = df_parsed.filter(col("temperature") > 30)

# Підготовка для Kafka (конвертуємо в JSON)
output_df = filtered_df.select(to_json(struct("*")).alias("value"))

# Записуємо у Kafka топік 'alerts-output'
query = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "alerts-output") \
    .option("checkpointLocation", "/tmp/checkpoints_filtered_demo") \
    .start()

query.awaitTermination()
