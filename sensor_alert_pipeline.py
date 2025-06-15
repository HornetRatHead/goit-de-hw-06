from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, expr, to_json, struct
from pyspark.sql.types import StructType, DoubleType, StringType, TimestampType

# --- 1. Ініціалізація Spark ---
spark = SparkSession.builder \
    .appName("SensorAlertPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# --- 2. Схема повідомлень із Kafka ---
sensor_schema = StructType() \
    .add("id", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("timestamp", StringType())

# --- 3. Зчитування потоку з Kafka ---
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "building_sensors") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), sensor_schema).alias("data")) \
    .selectExpr("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# --- 4. Sliding Window: середні значення температури та вологості ---
agg_df = df_parsed \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds")
    ) \
    .agg(
        {"temperature": "avg", "humidity": "avg"}
    ) \
    .withColumnRenamed("avg(temperature)", "t_avg") \
    .withColumnRenamed("avg(humidity)", "h_avg")

# --- 5. Зчитування алертів з CSV ---
alerts_df = spark.read.csv("alerts_conditions.csv", header=True, inferSchema=True)

# --- 6. Застосування умов алертів (crossJoin + фільтрація) ---
joined_df = agg_df.crossJoin(alerts_df)

alerts_detected = joined_df.where(
    "(min_temp = -999 OR t_avg >= min_temp) AND " +
    "(max_temp = -999 OR t_avg <= max_temp) AND " +
    "(min_hum = -999 OR h_avg >= min_hum) AND " +
    "(max_hum = -999 OR h_avg <= max_hum)"
)

# --- 7. Підготовка до запису у Kafka ---
alerts_ready = alerts_detected.select(
    col("window.start").cast("string").alias("start"),
    col("window.end").cast("string").alias("end"),
    "t_avg", "h_avg", "code", "message"
).withColumn("timestamp", expr("current_timestamp()")) \
 .select(to_json(struct("*")).alias("value"))

# --- 8. Запис у Kafka ---
kafka_query = alerts_ready.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "alerts-output") \
    .option("checkpointLocation", "/tmp/checkpoints_alerts_pipeline") \
    .outputMode("append") \
    .start()


# --- 9. Вивід у консоль (для перевірки) ---
console_query = alerts_ready.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

# --- Очікування завершення (всі потоки) ---
kafka_query.awaitTermination()
console_query.awaitTermination()
