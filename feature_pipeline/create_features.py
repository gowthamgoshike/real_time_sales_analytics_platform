import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# --- Database Credentials from .env ---
RDS_ENDPOINT = os.getenv("POSTGRES_HOST")
sales_feature_store = os.getenv("POSTGRES_DB")
postgres = os.getenv("POSTGRES_USER")
yourpassword = os.getenv("POSTGRES_PASSWORD")


# Use "postgres" as the db_name if you updated your .env
engine = create_engine(
    f"postgresql://{postgres}:{yourpassword}@{RDS_ENDPOINT}:5432/{sales_feature_store}?sslmode=require"
)

# --- UPDATED: Load Gold data from your specific S3 bucket ---
s3_path = "s3://sales-analytics-data-lake-23052021/gold/product_sales"

try:
    # This reads the parquet files directly from your S3 bucket
    df = pd.read_parquet(s3_path)
    print(f"Successfully loaded data from {s3_path}")

    # -------------------------
    # Feature Engineering
    # -------------------------
    df["avg_sale_per_product"] = df["total_revenue"] / 30
    df["high_revenue_product"] = (
        df["total_revenue"] > df["total_revenue"].mean()
    ).astype(int)

    # -------------------------
    # Save to Feature Store (RDS)
    # -------------------------
    df.to_sql(
        "product_features",
        engine,
        if_exists="replace",
        index=False
    )
    print("Features successfully stored in PostgreSQL RDS")

except Exception as e:
    print(f"Error: {e}")
    print("Verification Steps:")
    print(f"1. Run 'aws s3 ls {s3_path}/' to ensure data exists.")
    print("2. Ensure your .env file has the correct AWS_ACCESS_KEY and AWS_SECRET_KEY.")