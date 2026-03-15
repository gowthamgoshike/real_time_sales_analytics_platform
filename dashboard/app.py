import streamlit as st
import pandas as pd
import requests
from sqlalchemy import create_engine
import boto3
import pyarrow.parquet as pq
import io
import os
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# AWS S3 Access
# -----------------------------
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")
bucket_name = "sales-analytics-data-lake-23052021"

s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)


def load_gold_data():
    resp = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="gold/product_sales/")
    dfs = []
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        size = obj["Size"]
        # Skip zero-byte files
        if size == 0:
            continue
        data = s3_client.get_object(Bucket=bucket_name, Key=key)['Body'].read()
        dfs.append(pq.read_table(io.BytesIO(data)).to_pandas())
    
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        # No data yet, return empty DataFrame
        return pd.DataFrame()
gold_df = load_gold_data()
st.title("Real-Time Sales Analytics Dashboard")
st.subheader("Gold Layer: Product Sales Metrics")
st.dataframe(gold_df)

# -----------------------------
# Load Features from PostgreSQL
# -----------------------------
db_host = os.getenv("POSTGRES_HOST")
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")

engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}")
features_df = pd.read_sql("SELECT * FROM product_features", engine)
st.subheader("Feature Store: Product Features")
st.dataframe(features_df)

# -----------------------------
# ML Predictions via FastAPI
# -----------------------------
st.subheader("ML Predictions (FastAPI)")

product_id = st.selectbox("Select Product ID", gold_df["product_id"].unique())
feature_row = features_df[features_df["product_id"] == product_id].iloc[0]

api_url = os.getenv("FASTAPI_URL", "http://127.0.0.1:8000/predict")
response = requests.post(api_url, json={
    "avg_sale_per_product": float(feature_row["avg_sale_per_product"]),
    "high_revenue_product": int(feature_row["high_revenue_product"])
})
predicted_revenue = response.json()["predicted_revenue"]
st.metric(label=f"Predicted Revenue for Product {product_id}", value=f"${predicted_revenue:.2f}")