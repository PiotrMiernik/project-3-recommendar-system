"""
Recommender System Production Pipeline
======================================

This DAG orchestrates the end-to-end data pipeline for a hybrid recommender system.

The pipeline ingests raw product and review data, transforms it into cleaned staging
datasets, builds ML-ready feature tables, generates text embeddings from reviews,
loads embeddings into pgvector, and validates the final vector serving layer.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

from src.common.s3_utils import build_manifest_key


default_args = {
    "owner": "piotr",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


# ---------------------------------------------------------------------
# Global configuration
# ---------------------------------------------------------------------

EMR_STANDARD_APPLICATION_ID = "{{ var.value.emr_serverless_application_id }}"
EMR_EMBEDDINGS_APPLICATION_ID = "{{ var.value.emr_embeddings_application_id }}"
EMR_SERVERLESS_EXECUTION_ROLE_ARN = "{{ var.value.emr_serverless_runtime_role_arn }}"

AWS_CONN_ID = "aws_default"
AWS_REGION = "eu-central-1"

S3_BUCKET = "project-3-recommender-system"
S3_RAW_PREFIX = "raw/"
S3_STAGING_PREFIX = "staging/"
S3_MLREADY_PREFIX = "mlready/"

PY_FILES_S3_URI = f"s3://{S3_BUCKET}/jobs/src_package.zip"
EMR_LOGS_S3_URI = f"s3://{S3_BUCKET}/logs/emr-serverless/"

PRODUCTS_SCRIPT_S3_URI = f"s3://{S3_BUCKET}/jobs/transform_raw_products_to_staging.py"
REVIEWS_SCRIPT_S3_URI = f"s3://{S3_BUCKET}/jobs/transform_raw_reviews_to_staging.py"

MLREADY_PRODUCTS_SCRIPT = f"s3://{S3_BUCKET}/jobs/build_mlready_product_features.py"
MLREADY_USERS_SCRIPT = f"s3://{S3_BUCKET}/jobs/build_mlready_user_features.py"
MLREADY_STATS_SCRIPT = f"s3://{S3_BUCKET}/jobs/build_mlready_product_review_stats.py"
MLREADY_INTERACTIONS_SCRIPT = f"s3://{S3_BUCKET}/jobs/build_mlready_user_product_interactions.py"

PREPARE_REVIEWS_FOR_EMBEDDINGS_SCRIPT = (
    f"s3://{S3_BUCKET}/jobs/step1_prepare_and_filter_reviews.py"
)

# This script is located inside the custom Docker image used by the embeddings EMR application.
GENERATE_REVIEW_EMBEDDINGS_SCRIPT = "/opt/recommender/src/embeddings/step2_generate_review_embeddings.py"


# ---------------------------------------------------------------------
# Shared EMR configuration for standard Spark jobs
# ---------------------------------------------------------------------

COMMON_CONFIGURATION_OVERRIDES = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": EMR_LOGS_S3_URI,
        }
    },
    "applicationConfiguration": [
        {
            "classification": "spark-defaults",
            "properties": {
                "spark.submit.pyFiles": PY_FILES_S3_URI,
                "spark.emr-serverless.driverEnv.AWS_REGION": AWS_REGION,
                "spark.executorEnv.AWS_REGION": AWS_REGION,
                "spark.emr-serverless.driverEnv.S3_BUCKET": S3_BUCKET,
                "spark.executorEnv.S3_BUCKET": S3_BUCKET,
                "spark.emr-serverless.driverEnv.S3_RAW_PREFIX": S3_RAW_PREFIX,
                "spark.executorEnv.S3_RAW_PREFIX": S3_RAW_PREFIX,
                "spark.emr-serverless.driverEnv.S3_STAGING_PREFIX": S3_STAGING_PREFIX,
                "spark.executorEnv.S3_STAGING_PREFIX": S3_STAGING_PREFIX,
                "spark.emr-serverless.driverEnv.S3_MLREADY_PREFIX": S3_MLREADY_PREFIX,
                "spark.executorEnv.S3_MLREADY_PREFIX": S3_MLREADY_PREFIX,
                "spark.driver.extraJavaOptions": "-Djava.io.tmpdir=/tmp",
                "spark.executor.extraJavaOptions": "-Djava.io.tmpdir=/tmp",
                "spark.scratch.dir": "/tmp/spark-scratch",
            },
        }
    ],
}


# ---------------------------------------------------------------------
# EMR configuration for the Docker-based embeddings job
# ---------------------------------------------------------------------

EMBEDDINGS_CONFIGURATION_OVERRIDES = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": EMR_LOGS_S3_URI,
        }
    },
    "applicationConfiguration": [
        {
            "classification": "spark-defaults",
            "properties": {
                # Python runtime inside Docker image
                "spark.pyspark.python": "/usr/bin/python3",
                "spark.pyspark.driver.python": "/usr/bin/python3",

                # Make project modules importable from /opt/recommender
                "spark.emr-serverless.driverEnv.PYTHONPATH": "/opt/recommender",
                "spark.executorEnv.PYTHONPATH": "/opt/recommender",

                # AWS / S3 configuration
                "spark.emr-serverless.driverEnv.AWS_REGION": AWS_REGION,
                "spark.executorEnv.AWS_REGION": AWS_REGION,
                "spark.emr-serverless.driverEnv.S3_BUCKET": S3_BUCKET,
                "spark.executorEnv.S3_BUCKET": S3_BUCKET,
                "spark.emr-serverless.driverEnv.S3_STAGING_PREFIX": S3_STAGING_PREFIX,
                "spark.executorEnv.S3_STAGING_PREFIX": S3_STAGING_PREFIX,

                # Use the embedding model cached inside the Docker image
                "spark.emr-serverless.driverEnv.EMBEDDING_MODEL_VERSION": "/opt/recommender/models/all-MiniLM-L6-v2",
                "spark.executorEnv.EMBEDDING_MODEL_VERSION": "/opt/recommender/models/all-MiniLM-L6-v2",

                # Force offline mode to prevent runtime downloads from Hugging Face
                "spark.emr-serverless.driverEnv.TRANSFORMERS_OFFLINE": "1",
                "spark.executorEnv.TRANSFORMERS_OFFLINE": "1",
                "spark.emr-serverless.driverEnv.HF_HUB_OFFLINE": "1",
                "spark.executorEnv.HF_HUB_OFFLINE": "1",

                # Batch size for embedding inference
                "spark.emr-serverless.driverEnv.EMBEDDING_BATCH_SIZE": "32",
                "spark.executorEnv.EMBEDDING_BATCH_SIZE": "32",
            },
        }
    ],
}


# ---------------------------------------------------------------------
# Local Python task wrappers
# ---------------------------------------------------------------------

def ingest_products_wrapper(**kwargs):
    """Extract product metadata from PostgreSQL and write raw JSONL files to S3."""
    from src.ingestion.ingest_products_to_s3_raw import main

    return main()


def validate_staging_products_wrapper(ingest_dt, **kwargs):
    """Validate the cleaned product staging dataset before feature engineering."""
    from src.data_validation.validate_staging_products import (
        run_staging_products_validation,
    )

    return run_staging_products_validation(ingest_dt=ingest_dt)


def ingest_reviews_wrapper(**kwargs):
    """Extract review documents from MongoDB and write raw JSONL files to S3."""
    from src.ingestion.ingest_reviews_to_s3_raw import main

    return main()


def validate_staging_reviews_wrapper(ingest_dt, **kwargs):
    """Validate the cleaned review staging dataset before downstream aggregations."""
    from src.data_validation.validate_staging_reviews import (
        run_staging_reviews_validation,
    )

    return run_staging_reviews_validation(ingest_dt=ingest_dt)


def validate_mlready_product_features_wrapper(execution_date, **kwargs):
    """Validate ML-ready product-level features stored in the Gold layer."""
    from src.data_validation.validate_mlready_product_features import (
        run_mlready_product_features_validation,
    )

    return run_mlready_product_features_validation(execution_date=execution_date)


def validate_mlready_user_features_wrapper(execution_date, **kwargs):
    """Validate ML-ready user-level behavioral features."""
    from src.data_validation.validate_mlready_user_features import (
        run_mlready_user_features_validation,
    )

    return run_mlready_user_features_validation(execution_date=execution_date)


def validate_mlready_product_review_stats_wrapper(execution_date, **kwargs):
    """Validate product review statistics used by the recommender system."""
    from src.data_validation.validate_mlready_product_review_stats import (
        run_mlready_product_review_stats_validation,
    )

    return run_mlready_product_review_stats_validation(execution_date=execution_date)


def validate_mlready_user_product_interactions_wrapper(execution_date, **kwargs):
    """Validate user-product interaction features used for recommendation models."""
    from src.data_validation.validate_mlready_user_product_interactions import (
        run_mlready_user_product_interactions_validation,
    )

    return run_mlready_user_product_interactions_validation(
        execution_date=execution_date
    )


def load_to_pgvector_wrapper(ingest_dt, **kwargs):
    """Load generated review embeddings from S3 into PostgreSQL with pgvector."""
    from src.embeddings.step3_load_reviews_embeddings_to_pgvector import (
        run_load_review_embeddings_to_pgvector,
    )

    return run_load_review_embeddings_to_pgvector(ingest_dt=ingest_dt)


def validate_pgvector_wrapper(**kwargs):
    """Validate the pgvector serving table after loading review embeddings."""
    from src.data_validation.validate_review_embeddings import (
        validate_review_embeddings,
    )

    return validate_review_embeddings()


with DAG(
    dag_id="recommender_system_pipeline_prod",
    default_args=default_args,
    start_date=datetime(2026, 3, 23),
    schedule=None,
    catchup=False,
    tags=["project3", "prod", "recommender"],
) as dag:

    start = EmptyOperator(task_id="start")

    # -----------------------------------------------------------------
    # Phase 1: Raw ingestion and staging transformation
    # -----------------------------------------------------------------

    ingest_products = PythonOperator(
        task_id="ingest_products_to_s3_raw",
        python_callable=ingest_products_wrapper,
    )

    transform_products = EmrServerlessStartJobOperator(
        task_id="transform_raw_products_to_staging",
        application_id=EMR_STANDARD_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": PRODUCTS_SCRIPT_S3_URI,
                "entryPointArguments": [
                    build_manifest_key(entity="products", ingest_dt="{{ ds }}")
                ],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}",
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
    )

    validate_products = PythonOperator(
        task_id="validate_staging_products",
        python_callable=validate_staging_products_wrapper,
        op_kwargs={"ingest_dt": "{{ ds }}"},
    )

    ingest_reviews = PythonOperator(
        task_id="ingest_reviews_to_s3_raw",
        python_callable=ingest_reviews_wrapper,
    )

    transform_reviews = EmrServerlessStartJobOperator(
        task_id="transform_raw_reviews_to_staging",
        application_id=EMR_STANDARD_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": REVIEWS_SCRIPT_S3_URI,
                "entryPointArguments": [
                    build_manifest_key(entity="reviews", ingest_dt="{{ ds }}")
                ],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}",
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
    )

    validate_reviews = PythonOperator(
        task_id="validate_staging_reviews",
        python_callable=validate_staging_reviews_wrapper,
        op_kwargs={"ingest_dt": "{{ ds }}"},
    )

    # -----------------------------------------------------------------
    # Phase 2: ML-ready feature engineering
    # -----------------------------------------------------------------

    build_mlready_products = EmrServerlessStartJobOperator(
        task_id="build_mlready_product_features",
        application_id=EMR_STANDARD_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": MLREADY_PRODUCTS_SCRIPT,
                "entryPointArguments": [
                    "--input-path",
                    f"s3://{S3_BUCKET}/staging/products/ingest_dt={{{{ ds }}}}/",
                    "--execution-date",
                    "{{ ds }}",
                ],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}",
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
    )

    validate_ml_products = PythonOperator(
        task_id="validate_mlready_product_features",
        python_callable=validate_mlready_product_features_wrapper,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    build_mlready_users = EmrServerlessStartJobOperator(
        task_id="build_mlready_user_features",
        application_id=EMR_STANDARD_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": MLREADY_USERS_SCRIPT,
                "entryPointArguments": [
                    "--input-path",
                    f"s3://{S3_BUCKET}/staging/reviews/ingest_dt={{{{ ds }}}}/",
                    "--execution-date",
                    "{{ ds }}",
                ],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}",
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
    )

    validate_ml_users = PythonOperator(
        task_id="validate_mlready_user_features",
        python_callable=validate_mlready_user_features_wrapper,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    build_mlready_stats = EmrServerlessStartJobOperator(
        task_id="build_mlready_product_review_stats",
        application_id=EMR_STANDARD_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": MLREADY_STATS_SCRIPT,
                "entryPointArguments": [
                    "--input-path",
                    f"s3://{S3_BUCKET}/staging/reviews/ingest_dt={{{{ ds }}}}/",
                    "--execution-date",
                    "{{ ds }}",
                ],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}",
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
    )

    validate_ml_stats = PythonOperator(
        task_id="validate_mlready_product_review_stats",
        python_callable=validate_mlready_product_review_stats_wrapper,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    build_mlready_interactions = EmrServerlessStartJobOperator(
        task_id="build_mlready_user_product_interactions",
        application_id=EMR_STANDARD_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": MLREADY_INTERACTIONS_SCRIPT,
                "entryPointArguments": [
                    "--execution-date",
                    "{{ ds }}",
                ],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}",
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
    )

    validate_ml_interactions = PythonOperator(
        task_id="validate_mlready_user_product_interactions",
        python_callable=validate_mlready_user_product_interactions_wrapper,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    # -----------------------------------------------------------------
    # Phase 3: Review text preparation and embedding generation
    # -----------------------------------------------------------------

    prep_embeddings = EmrServerlessStartJobOperator(
        task_id="prepare_and_filter_reviews_for_embeddings",
        application_id=EMR_STANDARD_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": PREPARE_REVIEWS_FOR_EMBEDDINGS_SCRIPT,
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}",
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
    )

    gen_embeddings = EmrServerlessStartJobOperator(
        task_id="generate_review_embeddings",
        application_id=EMR_EMBEDDINGS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": GENERATE_REVIEW_EMBEDDINGS_SCRIPT,
                "sparkSubmitParameters": (
                    "--conf spark.emr-serverless.driver.cpu=4vCPU "
                    "--conf spark.emr-serverless.driver.memory=16G "
                    "--conf spark.emr-serverless.driver.disk=100G "
                    "--conf spark.emr-serverless.executor.cpu=4vCPU "
                    "--conf spark.emr-serverless.executor.memory=16G "
                    "--conf spark.emr-serverless.executor.disk=100G "
                    "--conf spark.executor.instances=3 "
                    "--conf spark.dynamicAllocation.enabled=false "
                    "--conf spark.sql.execution.arrow.pyspark.enabled=true"
                ),
            }
        },
        configuration_overrides=EMBEDDINGS_CONFIGURATION_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
        waiter_delay=60,
        waiter_max_attempts=180,
    )

    # -----------------------------------------------------------------
    # Phase 4: Vector serving layer
    # -----------------------------------------------------------------

    load_pg = PythonOperator(
        task_id="load_review_embeddings_to_pgvector",
        python_callable=load_to_pgvector_wrapper,
        op_kwargs={"ingest_dt": "{{ ds }}"},
    )

    validate_pg = PythonOperator(
        task_id="validate_pgvector_review_embeddings",
        python_callable=validate_pgvector_wrapper,
    )

    end = EmptyOperator(task_id="end")

    # -----------------------------------------------------------------
    # Pipeline orchestration
    # -----------------------------------------------------------------

    (
        start
        >> ingest_products
        >> transform_products
        >> validate_products
        >> ingest_reviews
        >> transform_reviews
        >> validate_reviews
        >> build_mlready_products
        >> validate_ml_products
        >> build_mlready_users
        >> validate_ml_users
        >> build_mlready_stats
        >> validate_ml_stats
        >> build_mlready_interactions
        >> validate_ml_interactions
        >> prep_embeddings
        >> gen_embeddings
        >> load_pg
        >> validate_pg
        >> end
    )