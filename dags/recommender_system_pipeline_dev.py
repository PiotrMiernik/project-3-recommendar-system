# This DAG orchestrates the recommender system pipeline:
# - First it runs two parallel ingestion tasks:
#   PostgreSQL products_raw -> S3 Raw and MongoDB reviews_raw -> S3 Raw
# - Then it runs two local transformation tasks:
#   S3 Raw -> S3 Staging for products and reviews
# - Next, it runs two local Great Expectations validation tasks on staging data

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from src.ingestion.ingest_products_to_s3_raw import main as ingest_products_main
from src.ingestion.ingest_reviews_to_s3_raw import main as ingest_reviews_main
from src.common.s3_utils import build_manifest_key
from src.transformations.transform_raw_products_to_staging import run_products_transformation
from src.transformations.transform_raw_reviews_to_staging import run_reviews_transformation
from src.data_validation.validate_staging_products import run_staging_products_validation
from src.data_validation.validate_staging_reviews import run_staging_reviews_validation


default_args = {
    "owner": "piotr",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_transform_products_task(**context):
    """
    Airflow wrapper for local products transformation.
    It resolves manifest_key from Airflow execution date
    and passes it to the transformation function.
    """
    ingest_dt = context["ds"]
    manifest_key = build_manifest_key(entity="products", ingest_dt=ingest_dt)
    run_products_transformation(manifest_key=manifest_key)


def run_transform_reviews_task(**context):
    """
    Airflow wrapper for local reviews transformation.
    It resolves manifest_key from Airflow execution date
    and passes it to the transformation function.
    """
    ingest_dt = context["ds"]
    manifest_key = build_manifest_key(entity="reviews", ingest_dt=ingest_dt)
    run_reviews_transformation(manifest_key=manifest_key)


def run_validate_products_task(**context):
    """
    Airflow wrapper for Great Expectations validation
    of staging products data.
    """
    ingest_dt = context["ds"]
    run_staging_products_validation(ingest_dt=ingest_dt)


def run_validate_reviews_task(**context):
    """
    Airflow wrapper for Great Expectations validation
    of staging reviews data.
    """
    ingest_dt = context["ds"]
    run_staging_reviews_validation(ingest_dt=ingest_dt)


with DAG(
    dag_id="recommender_system_pipeline",
    description="Main orchestration DAG for the recommender system project",
    default_args=default_args,
    start_date=datetime(2026, 3, 23),
    schedule=None,
    catchup=False,
    tags=["project3", "recommender", "pipeline", "dev"],
) as dag:

    # Dummy start task used to make the DAG structure clearer
    start = EmptyOperator(task_id="start")

    # Ingest products from PostgreSQL to S3 Raw
    ingest_products = PythonOperator(
        task_id="ingest_products_to_s3_raw",
        python_callable=ingest_products_main,
    )

    # Ingest reviews from MongoDB to S3 Raw
    ingest_reviews = PythonOperator(
        task_id="ingest_reviews_to_s3_raw",
        python_callable=ingest_reviews_main,
    )

    # Transform products from Raw to Staging locally
    transform_products = PythonOperator(
        task_id="transform_raw_products_to_staging",
        python_callable=run_transform_products_task,
    )

    # Transform reviews from Raw to Staging locally
    transform_reviews = PythonOperator(
        task_id="transform_raw_reviews_to_staging",
        python_callable=run_transform_reviews_task,
    )

    # Validate staging products locally with Great Expectations
    validate_products = PythonOperator(
        task_id="validate_staging_products",
        python_callable=run_validate_products_task,
    )

    # Validate staging reviews locally with Great Expectations
    validate_reviews = PythonOperator(
        task_id="validate_staging_reviews",
        python_callable=run_validate_reviews_task,
    )

    # Dummy end task that waits for both validation branches
    end = EmptyOperator(task_id="end")

    start >> [ingest_products, ingest_reviews]

    ingest_products >> transform_products >> validate_products
    ingest_reviews >> transform_reviews >> validate_reviews

    [validate_products, validate_reviews] >> end