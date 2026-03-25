# This DAG orchestrates the recommender system pipeline:
    # - First it runs two parallel ingestion tasks: PostgreSQL products_raw -> S3 Raw and MongoDB reviews_raw -> S3 Raw.
    # -  

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from src.ingestion.ingest_products_to_s3_raw import main as ingest_products_main
from src.ingestion.ingest_reviews_to_s3_raw import main as ingest_reviews_main


default_args = {
    "owner": "piotr",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id = "recommender_system_pipeline",
    description = "Main orchestration DAG for the recommender system project",
    default_args=default_args,
    start_date=datetime(2026, 3, 23),
    schedule=None,
    catchup=False,
    tags=["project3", "recommender", "pipeline"],
) as dag:
    
    # Dummy start task used to make the DAG structure clearer
    start = EmptyOperator(task_id="start")

    # Ingest products from PostreSQL to S3 Raw
    ingest_products = PythonOperator(
        task_id="ingest_products_to_s3_raw",
        python_callable=ingest_products_main
    )

    # Ingest reviews from MongoDB to S3 Raw
    ingest_reviews = PythonOperator(
        task_id="ingest_reviews_to_s3_raw",
        python_callable=ingest_reviews_main,
    )

    # Dummy end task that will wait for both ingestion tasks to finish
    end = EmptyOperator(task_id="end")

    start >> [ingest_products, ingest_reviews] >> end