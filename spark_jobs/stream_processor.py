import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum, to_timestamp
from pyspark.sql.types import (
    StructType, IntegerType, DoubleType, StringType, TimestampType
)


load_dotenv()

aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")

if not aws_access_key or not aws_secret_key:
    raise ValueError("❌ AWS Credentials not found in .env file")


spark = SparkSession.builder \
    .appName("RealTimeSalesMedallionPipeline") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .getOrCreate()

# Set AWS credentials
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

spark.sparkContext.setLogLevel("WARN")


schema = StructType() \
    .add("transaction_id", IntegerType()) \
    .add("product_id", IntegerType()) \
    .add("store_id", IntegerType()) \
    .add("price", DoubleType()) \
    .add("quantity", IntegerType()) \
    .add("timestamp", StringType())


kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_topic") \
    .option("startingOffsets", "latest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING)")

sales_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Convert timestamp string → proper timestamp type
sales_df = sales_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

sales_df = sales_df.withColumn(
    "total_sale",
    col("price") * col("quantity")
)

# Reduce small files
sales_df = sales_df.repartition(2)


bronze_query = sales_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://sales-analytics-data-lake-23052021/bronze/sales") \
    .option("checkpointLocation",
            "s3a://sales-analytics-data-lake-23052021/checkpoints/bronze") \
    .outputMode("append") \
    .start()


silver_df = sales_df \
    .dropDuplicates(["transaction_id"]) \
    .filter(col("price") > 0)

silver_df = silver_df.repartition(2)

silver_query = silver_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://sales-analytics-data-lake-23052021/silver/sales") \
    .option("checkpointLocation",
            "s3a://sales-analytics-data-lake-23052021/checkpoints/silver") \
    .outputMode("append") \
    .start()


gold_df = silver_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy("product_id") \
    .agg(
        sum("total_sale").alias("total_revenue")
    )

# Write each micro-batch safely
def write_gold_layer(batch_df, batch_id):

    batch_df.write \
        .format("parquet") \
        .mode("append") \
        .save("s3a://sales-analytics-data-lake-23052021/gold/product_sales")
gold_query = gold_df.writeStream \
    .foreachBatch(write_gold_layer) \
    .outputMode("complete") \
    .option("checkpointLocation",
            "s3a://sales-analytics-data-lake-23052021/checkpoints/gold") \
    .start()


spark.streams.awaitAnyTermination()