from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

PROJECT_DIR = "/Users/gowthamgoshike/projects/real_time_sales_analytics_platform"
VENV_ACTIVATE = "/Users/gowthamgoshike/projects/gowthamvenv/bin/activate"

default_args = {
    "owner": "gowtham",
    "retries": 1
}

dag = DAG(
    dag_id="real_time_sales_pipeline",
    default_args=default_args,
    description="Real-Time Sales Medallion Pipeline",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False
)

spark_streaming = BashOperator(
    task_id="spark_streaming_job",
    # Added a space at the end of the string to prevent TemplateNotFound error
    bash_command=f"source {VENV_ACTIVATE} && cd {PROJECT_DIR} && ./runspark.sh ",
    dag=dag
)

feature_engineering = BashOperator(
    task_id="feature_engineering",
    bash_command=f"source {VENV_ACTIVATE} && cd {PROJECT_DIR} && python feature_pipeline/create_features.py",
    dag=dag
)

ml_training = BashOperator(
    task_id="ml_training",
    bash_command=f"source {VENV_ACTIVATE} && cd {PROJECT_DIR} && python ml_pipeline/train_model.py",
    dag=dag
)

refresh_api = BashOperator(
    task_id="refresh_prediction_api",
    bash_command=f"source {VENV_ACTIVATE} && cd {PROJECT_DIR} && pkill -f uvicorn || true && nohup uvicorn api.main:app --host 0.0.0.0 --port 8000 & ",
    dag=dag
)


# -----------------------------
# Task dependencies
# -----------------------------
spark_streaming >> feature_engineering >> ml_training >> refresh_api 


