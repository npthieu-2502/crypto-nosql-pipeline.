import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, max, min, first, last, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from dotenv import load_dotenv

# Load cấu hình từ file .env
load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'localhost')
CASSANDRA_PORT = os.getenv('CASSANDRA_PORT', '9042')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("CryptoRealTimePipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa Schema
schema = StructType([
    StructField("event_time", LongType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True)
])

# 3. Đọc dữ liệu Streaming từ Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "crypto_trades") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

timestamped_df = parsed_df.withColumn("timestamp", from_unixtime(col("event_time") / 1000).cast("timestamp"))

# 4. Tính toán OHLC
ohlc_df = timestamped_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("symbol")
    ) \
    .agg(
        first("price").alias("open_price"),
        max("price").alias("high_price"),
        min("price").alias("low_price"),
        last("price").alias("close_price")
    )

# 5. Hàm ghi dữ liệu vào Cassandra
def write_to_cassandra(batch_df, batch_id):
    formatted_df = batch_df.select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("open_price"),
        col("high_price"),
        col("low_price"),
        col("close_price")
    )
    if not formatted_df.isEmpty():
        formatted_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="ohlc_1m", keyspace="crypto_ks") \
            .mode("append") \
            .save()
        print(f"Batch {batch_id} written to Cassandra")

# 6. Hàm ghi dữ liệu giá hiện tại vào Redis
def write_to_redis(batch_df, batch_id):
    import redis
    if batch_df.isEmpty(): return
    
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    latest_prices = batch_df.groupBy("symbol").agg(last("price").alias("latest_price")).collect()
    
    for row in latest_prices:
        r.set(f"live_price:{row.symbol}", row.latest_price)
    
    print(f"Batch {batch_id} updated live prices in Redis")

# 7. Start Streaming Queries (Đã thêm Checkpoint)
cassandra_query = ohlc_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("update") \
    .option("checkpointLocation", "./spark_checkpoints/cassandra") \
    .start()

redis_query = timestamped_df.writeStream \
    .foreachBatch(write_to_redis) \
    .outputMode("update") \
    .option("checkpointLocation", "./spark_checkpoints/redis") \
    .start()

print("Spark Streaming Job Started with Checkpointing...")
spark.streams.awaitAnyTermination()
