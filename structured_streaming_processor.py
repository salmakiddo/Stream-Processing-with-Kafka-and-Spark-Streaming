from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max, min, count, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def create_spark_session():
    return SparkSession.builder \
        .appName("IoT Structured Streaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

def define_sensor_schema():
    # Schema matches producer's JSON structure
    return StructType([
        StructField("sensor_id", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("location", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("quality", StringType(), True)
    ])

def process_stream():
    print("="*60)
    print("Starting Structured Streaming Processor")
    print("="*60)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = define_sensor_schema()

    # Read stream from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "iot-sensors") \
        .option("startingOffsets", "latest") \
        .option("group.id", "iot-analytics") \
        .load()

    # Parse JSON and cast timestamp
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("metric_value", col("value").cast(DoubleType())) \
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # Add watermark to handle late data
    watermarked_df = parsed_df.withWatermark("timestamp", "2 minutes")

    # Aggregation per 30-second window and sensor_type
    window_stats = watermarked_df.groupBy(
        window(col("timestamp"), "30 seconds"),
        col("sensor_type")
    ).agg(
        count("*").alias("message_count"),
        avg("metric_value").alias("avg_value"),
        max("metric_value").alias("max_value"),
        min("metric_value").alias("min_value")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "sensor_type",
        "message_count",
        "avg_value",
        "max_value",
        "min_value"
    )

    # Alerts for high temperature or low quality readings
    alerts_df = watermarked_df.filter(
        (col("sensor_type") == "temperature") & (col("metric_value") > 35.0)
    ).select(
        "timestamp", "sensor_id", "sensor_type", "metric_value", "location",
    ).withColumnRenamed("metric_value", "temperature_value")

    # Query 1: Window statistics to console
    query1 = window_stats.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .option("checkpointLocation", "/tmp/spark-iot-checkpoint") \
        .queryName("WindowStatistics") \
        .start()

    # Query 2: Alerts to console
    query2 = alerts_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .queryName("Alerts") \
        .start()

    print("="*60)
    print("Streaming Processor Running. Press Ctrl+C to stop.")
    print("Monitor Spark UI at: http://localhost:4040")
    print("="*60)

    try:
        query1.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming queries...")
        query1.stop()
        query2.stop()
        spark.stop()
        print("Shutdown complete.")

if __name__ == "__main__":
    process_stream()
