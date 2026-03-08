import pandas as pd
import os
import joblib
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
# Updated import for newer scikit-learn versions
from sklearn.metrics import root_mean_squared_error 
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Database Connection (PostgreSQL RDS) ---
db_host = os.getenv("POSTGRES_HOST")
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")

# Use ?sslmode=require for AWS RDS connection
engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}?sslmode=require")

def train_sales_model():
    # 1. Load Features from RDS Feature Store
    print("Fetching features from PostgreSQL RDS...")
    query = "SELECT * FROM product_features"
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("Error: No data found in product_features table. Run create_features.py first.")
        return

    print("Loaded Features (first 5 rows):")
    print(df.head())

    # 2. Prepare Data
    # Features: engineered in Day 5
    # Target: total_revenue
    X = df[["avg_sale_per_product", "high_revenue_product"]]
    y = df["total_revenue"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # 3. Train Model
    print("Training Random Forest model...")
    model = RandomForestRegressor(n_estimators=100, max_depth=5, random_state=42)
    model.fit(X_train, y_train)

    # 4. Evaluate (Using the new root_mean_squared_error function)
    predictions = model.predict(X_test)
    rmse = root_mean_squared_error(y_test, predictions)
    print(f"Model Training Complete. RMSE: {rmse:.2f}")

    # 5. Save Model Locally
    os.makedirs("models", exist_ok=True)
    model_path = "models/sales_model.pkl"
    joblib.dump(model, model_path)
    print(f"Model saved successfully to {model_path}")

if __name__ == "__main__":
    train_sales_model()