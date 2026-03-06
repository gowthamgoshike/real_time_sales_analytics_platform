from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth
from pyspark.sql.types import StructType, IntegerType, DoubleType, StringType

spark = SparkSession.builder \
    .appName("RealTimeSalesProcessing") \
    .config("spark.sql.streaming.fileSink.log.deletion", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema
schema = StructType() \
    .add("transaction_id", IntegerType()) \
    .add("product_id", IntegerType()) \
    .add("store_id", IntegerType()) \
    .add("price", DoubleType()) \
    .add("quantity", IntegerType()) \
    .add("timestamp", StringType())

# Read Kafka stream
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value to JSON
json_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Parse JSON
sales_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Feature engineering
sales_df = sales_df.withColumn(
    "total_sale",
    col("price") * col("quantity")
)

# Add partitions for data lake
sales_df = sales_df \
    .withColumn("year", year("timestamp")) \
    .withColumn("month", month("timestamp")) \
    .withColumn("day", dayofmonth("timestamp"))

# Write to Data Lake
query = sales_df.writeStream \
    .format("parquet") \
    .option("path", "data/sales_data") \
    .option("checkpointLocation", "data/checkpoints") \
    .partitionBy("year", "month", "day") \
    .outputMode("append") \
    .start()

query.awaitTermination()