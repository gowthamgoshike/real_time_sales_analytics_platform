# Real-Time Sales Analytics Platform

An end-to-end **Real-Time Data Engineering and MLOps Platform** built using modern industry tools.  
This project demonstrates how to build a **production-grade streaming data pipeline, feature store, machine learning pipeline, and real-time analytics dashboard**.

The platform simulates real-time sales transactions, processes them using distributed systems, generates features, trains ML models, and serves predictions through an API and analytics dashboard.

---

# Architecture

Sales Data Generator

↓

Apache Kafka (Streaming Layer)

↓

Apache Spark Streaming

↓

AWS S3 Data Lake
Bronze → Silver → Gold

↓

Feature Engineering Pipeline

↓

PostgreSQL Feature Store

↓

ML Training Pipeline (MLflow Tracking)

↓

FastAPI Model Serving

↓

Streamlit Analytics Dashboard

Workflow orchestration is handled using **Apache Airflow DAGs**.

---

# Tech Stack

### Data Engineering
- Apache Kafka
- Apache Spark (Structured Streaming)
- AWS S3 Data Lake
- PostgreSQL

### Machine Learning
- Scikit-learn
- MLflow
- Feature Engineering Pipelines

### Backend / APIs
- FastAPI
- Uvicorn

### Analytics
- Streamlit Dashboard

### Orchestration
- Apache Airflow

### Infrastructure
- Docker
- AWS Free Tier

---

# Project Structure

real_time_sales_analytics_platform/

api/

  main.py                     # FastAPI model serving API

airflow/

  dags/

  sales_ml_pipeline.py    # Airflow DAG orchestration

dashboard/

  app.py                      # Streamlit analytics dashboard

data_generator/

  generate_sales.py           # Simulated real-time sales data

spark_jobs/

  stream_processor.py         # Spark streaming pipeline

feature_pipeline/

  create_features.py          # Feature engineering

ml_pipeline/

  train_model.py              # ML training pipeline

models/

  sales_prediction_model.pkl  # Trained model

kafka/

  consumer.py                 # Kafka consumer

terraform/

  infrastructure provisioning

docker/

  docker-compose.yml

configs/

  project configurations

tests/

---

# Medallion Data Architecture

The pipeline follows a **Medallion Architecture** used in modern data platforms.

### Bronze Layer
Raw streaming sales data stored in S3.

### Silver Layer
Cleaned and processed transactional data.

### Gold Layer
Aggregated analytics tables used for ML training and dashboards.

---

# Key Features

- Real-time streaming pipeline
- Distributed data processing
- Feature store architecture
- Machine learning model training
- Experiment tracking with MLflow
- REST API for model predictions
- Real-time analytics dashboard
- Workflow orchestration with Airflow
- Containerized services with Docker
- Infrastructure as Code using Terraform

---

# Installation

Clone the repository:

https://github.com/gowthamgoshike/real_time_sales_analytics_platform.git

cd real_time_sales_analytics_platform

Install dependencies:

pip install -r requirements.txt

---

# Environment Variables

Create a `.env` file in the root directory.

Example:

POSTGRES_HOST=localhost

POSTGRES_DB=sales_feature_store

POSTGRES_USER=postgres

POSTGRES_PASSWORD=postgres

AWS_ACCESS_KEY=your_access_key

AWS_SECRET_KEY=your_secret_key

AWS_BUCKET=sales-data-lake

---

# Running the Platform

## 1 Start Kafka

docker-compose up kafka

---

## 2 Start Sales Data Generator

python data_generator/generate_sales.py

--This simulates real-time sales transactions.

---

## 3 Start Spark Streaming Job

python spark_jobs/stream_processor.py

--Processes streaming data and stores it in the data lake.

---

## 4 Run Feature Engineering

python feature_pipeline/create_features.py

--Creates ML features and stores them in PostgreSQL.

---

## 5 Train Machine Learning Model

python ml_pipeline/train_model.py

--The trained model will be stored inside:

models/sales_prediction_model.pkl

--ML experiments are tracked using **MLflow**.

---

## 6 Start Model Serving API

uvicorn api.main:app –reload

API available at:

http://localhost:8000

Swagger documentation:

http://localhost:8000/docs

---

## 7 Launch Streamlit Dashboard

streamlit run dashboard/app.py

Dashboard URL:

http://localhost:8501

The dashboard shows:

- Sales metrics
- Product analytics
- Revenue predictions

---

# Airflow Pipeline

Airflow automates the ML pipeline using DAGs.

Pipeline tasks:

spark_stream_processing

↓

feature_engineering

↓

train_ml_model

↓

serve_model_api

Start Airflow:

airflow scheduler
airflow webserver –port 8080

Open UI:

http://localhost:8080

---


# Future Improvements

- Real-time model retraining
- Feature store versioning
- Kafka streaming dashboard updates
- Kubernetes deployment
- Data quality monitoring
- Advanced ML models

---

# Skills Demonstrated

- Data Engineering
- Streaming Architectures
- Feature Engineering
- MLOps Pipelines
- Model Serving
- Workflow Orchestration
- Cloud Data Platforms

---

# Author

Gowtham Goshike

Master’s in Computer Science  
 Full Stack Data Science | Machine Learning | MLOps


