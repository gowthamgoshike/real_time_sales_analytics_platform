from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import numpy as np

app = FastAPI(
    title="Sales Prediction API",
    description="Predict product sales revenue",
    version="1.0"
)

# Load trained model
model = joblib.load("models/sales_prediction_model.pkl")


class SalesFeatures(BaseModel):
    avg_sale_per_product: float
    high_revenue_product: int


@app.get("/")
def home():
    return {"message": "Sales Prediction API Running"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/predict")
def predict(data: SalesFeatures):

    features = np.array([[data.avg_sale_per_product,
                          data.high_revenue_product]])

    prediction = model.predict(features)

    return {
        "predicted_revenue": float(prediction[0])
    }