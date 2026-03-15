#!/usr/bin/env python3
import os
import pandas as pd
import s3fs
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ---------------------------------------------------------
# 1. Configuration & Environment Setup
# ---------------------------------------------------------
load_dotenv()

# Database Credentials
db_host = os.getenv("POSTGRES_HOST")
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")

# AWS Credentials
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")


# Passing this object to pandas instead of a string fixes the ImportError.
db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_name}"
engine = create_engine(db_url)

print("Connecting to S3 to find parquet files...")

fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)
s3_folder = "sales-analytics-data-lake-23052021/gold/product_sales/"
file_paths = fs.glob(f"{s3_folder}*.parquet")

if not file_paths:
    raise FileNotFoundError(f"No parquet files found in s3://{s3_folder}")

print(f"Found {len(file_paths)} files. Loading data into memory...")
df = pd.read_parquet(file_paths, filesystem=fs, engine='pyarrow')
print(f"Successfully loaded {len(df)} rows.")


df["total_revenue"] = df["total_revenue"].astype(float)

# Deduplicate to keep only one row per product_id
df = df.sort_values(by="total_revenue", ascending=False)
df = df.drop_duplicates(subset=["product_id"], keep="first")

# Perform calculations on the clean dataset
df["avg_sale_per_product"] = df["total_revenue"] / 30
df["high_revenue_product"] = (
    df["total_revenue"] > df["total_revenue"].mean()
).astype(int)

print(f"Cleaned data: {len(df)} unique products remaining.")

print("Storing features in PostgreSQL 'product_features' table...")

# Use the engine directly, but wrap the execution
df.to_sql(
    name="product_features",
    con=engine,             # Use the engine object
    if_exists="replace",
    index=False,
    method="multi"          # This often forces pandas to use a different execution path
)

print("✅ Success: Features successfully stored in PostgreSQL.")