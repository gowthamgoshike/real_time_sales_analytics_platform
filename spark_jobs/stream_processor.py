from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum
from pyspark.sql.types import StructType, IntegerType, DoubleType, StringType

def main():
    # 1. Initialize Spark Session 
    # (Spark 4.1.1 will automatically use Scala 2.13)
    spark = SparkSession.builder \
        .appName("RealTimeSalesAnalytics") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    # Set logging level to WARN to reduce console noise
    spark.sparkContext.setLogLevel("WARN")

    # 2. Define the Schema for incoming JSON data
    schema = StructType() \
        .add("transaction_id", IntegerType()) \
        .add("product_id", IntegerType()) \
        .add("store_id", IntegerType()) \
        .add("price", DoubleType()) \
        .add("quantity", IntegerType()) \
        .add("timestamp", StringType())

    # 3. Read Stream from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sales_topic") \
        .option("startingOffsets", "latest") \
        .load()

    # 4. Parse JSON and Transform
    # Extract the 'value' from Kafka and apply the schema
    sales_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("total_sale", col("price") * col("quantity"))

    # 5. Create Aggregation (Revenue per Product)
    sales_agg = sales_df.groupBy("product_id") \
        .agg(sum("total_sale").alias("total_revenue"))

    # ---------------------------------------------------------
    # SINK 1: Write RAW processed data to Parquet (Data Lake)
    # ---------------------------------------------------------
    query_parquet = sales_df.writeStream \
        .format("parquet") \
        .option("path", "data/processed/sales_raw") \
        .option("checkpointLocation", "data/checkpoints/sales_raw") \
        .outputMode("append") \
        .start()

    # ---------------------------------------------------------
    # SINK 2: Show Aggregated Revenue in Console
    # ---------------------------------------------------------
    query_console = sales_agg.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("checkpointLocation", "data/checkpoints/sales_agg") \
        .start()

    # 6. Keep the application running
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()