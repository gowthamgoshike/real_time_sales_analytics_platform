#!/bin/bash
# Clean up old broken state
#rm -rf data/checkpoints data/processed
# Run spark with enough memory
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  spark_jobs/stream_processor.py