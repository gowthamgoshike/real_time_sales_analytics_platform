import pandas as pd
import joblib
import mlflow
import mlflow.sklearn
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import root_mean_squared_error
import os
# ------------------------------
# Database Connection
# ------------------------------

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
# ------------------------------
# Load Features
# ------------------------------

query = "SELECT * FROM product_features"

df = pd.read_sql(query, engine)

print("Loaded Features:")
print(df.head())

# ------------------------------
# Prepare Dataset
# ------------------------------

X = df[["avg_sale_per_product", "high_revenue_product"]]

y = df["total_revenue"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# ------------------------------
# MLflow Tracking
# ------------------------------

mlflow.set_experiment("sales_prediction")

with mlflow.start_run():

    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=5
    )

    model.fit(X_train, y_train)

    predictions = model.predict(X_test)

    rmse = root_mean_squared_error(y_test, predictions)
    print(f"Model RMSE: {rmse}")
    
    model_dir = 'models/model'
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)
        print(f"Created directory: {model_dir}")

# 2. Define the full path for the pickle file
    model_path = os.path.join(model_dir, 'sales_prediction_model.pkl')

# 3. Save the model
    joblib.dump(model, model_path)

    print(f"Model saved successfully to {model_path}")
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 5)

    mlflow.log_metric("rmse", rmse)

    mlflow.sklearn.log_model(
        model,
        "sales_prediction_model"
    )