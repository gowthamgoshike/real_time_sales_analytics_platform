#!/bin/bash
# Clean up old broken state
#rm -rf data/checkpoints data/processed
# Run spark with enough memory
spark-submit \
  --driver-memory 2g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  spark_jobs/stream_processor.py