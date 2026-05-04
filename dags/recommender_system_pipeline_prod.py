# This DAG orchestrates the production version of the recommender system pipeline.
# All local Python tasks use "Lazy Imports" to prevent Airflow DAG parsing timeouts
# caused by heavy ML libraries (torch, sentence-transformers) or database drivers.
# The entire pipeline is executed sequentially to avoid EMR/Spark resource contention.
#
# Changes:
# - Switched from .tar.gz environment to native ECR Docker Image support.
# - Optimized resource allocation (CPU/RAM) for heavy ML embedding tasks.
# - Cleaned up Spark parameters by removing redundant environment paths.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

# Lightweight utility imports
from src.common.s3_utils import build_manifest_key

default_args = {
    "owner": "piotr",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

# Airflow Variables via Jinja templates to prevent DB thrashing
EMR_SERVERLESS_APPLICATION_ID = "{{ var.value.emr_serverless_application_id }}"
EMR_SERVERLESS_EXECUTION_ROLE_ARN = "{{ var.value.emr_serverless_runtime_role_arn }}"

AWS_CONN_ID = "aws_default"
S3_BUCKET = "project-3-recommender-system"
ECR_IMAGE_URI = "524501562188.dkr.ecr.eu-central-1.amazonaws.com/recommender-embeddings:latest"

# S3 Path configurations
PRODUCTS_SCRIPT_S3_URI = f"s3://{S3_BUCKET}/jobs/transform_raw_products_to_staging.py"
REVIEWS_SCRIPT_S3_URI = f"s3://{S3_BUCKET}/jobs/transform_raw_reviews_to_staging.py"
MLREADY_PRODUCTS_SCRIPT = f"s3://{S3_BUCKET}/jobs/build_mlready_product_features.py"
MLREADY_USERS_SCRIPT = f"s3://{S3_BUCKET}/jobs/build_mlready_user_features.py"
MLREADY_STATS_SCRIPT = f"s3://{S3_BUCKET}/jobs/build_mlready_product_review_stats.py"
MLREADY_INTERACTIONS_SCRIPT = f"s3://{S3_BUCKET}/jobs/build_mlready_user_product_interactions.py"
PREPARE_REVIEWS_FOR_EMBEDDINGS_SCRIPT = f"s3://{S3_BUCKET}/jobs/step1_prepare_and_filter_reviews.py"
GENERATE_REVIEW_EMBEDDINGS_SCRIPT = f"s3://{S3_BUCKET}/jobs/step2_generate_review_embeddings.py"

PY_FILES_S3_URI = f"s3://{S3_BUCKET}/jobs/src_package.zip"
EMR_LOGS_S3_URI = f"s3://{S3_BUCKET}/logs/emr-serverless/"

# Global configuration for EMR Serverless jobs
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
                "spark.emr-serverless.driverEnv.AWS_REGION": "eu-central-1",
                "spark.emr-serverless.driverEnv.S3_BUCKET": S3_BUCKET,
                "spark.emr-serverless.driverEnv.S3_RAW_PREFIX": "raw/",
                "spark.emr-serverless.driverEnv.S3_STAGING_PREFIX": "staging/",
                "spark.emr-serverless.driverEnv.S3_MLREADY_PREFIX": "mlready/",
                "spark.emr-serverless.driverEnv.EMBEDDING_MODEL_VERSION": "all-MiniLM-L6-v2",
                "spark.emr-serverless.driverEnv.EMBEDDING_BATCH_SIZE": "32",
                "spark.executorEnv.AWS_REGION": "eu-central-1",
                "spark.executorEnv.S3_BUCKET": S3_BUCKET,
                "spark.executorEnv.S3_RAW_PREFIX": "raw/",
                "spark.executorEnv.S3_STAGING_PREFIX": "staging/",
                "spark.executorEnv.S3_MLREADY_PREFIX": "mlready/",
                "spark.executorEnv.EMBEDDING_MODEL_VERSION": "all-MiniLM-L6-v2",
                "spark.executorEnv.EMBEDDING_BATCH_SIZE": "32",
            },
        }
    ],
    "imageConfiguration": {
        "imageUri": ECR_IMAGE_URI
    }
}

# --- Lazy Loading Wrapper Functions ---

def ingest_products_wrapper(**kwargs):
    from src.ingestion.ingest_products_to_s3_raw import main
    return main()

def validate_staging_products_wrapper(ingest_dt, **kwargs):
    from src.data_validation.validate_staging_products import run_staging_products_validation
    return run_staging_products_validation(ingest_dt=ingest_dt)

def ingest_reviews_wrapper(**kwargs):
    from src.ingestion.ingest_reviews_to_s3_raw import main
    return main()

def validate_staging_reviews_wrapper(ingest_dt, **kwargs):
    from src.data_validation.validate_staging_reviews import run_staging_reviews_validation
    return run_staging_reviews_validation(ingest_dt=ingest_dt)

def validate_mlready_product_features_wrapper(execution_date, **kwargs):
    from src.data_validation.validate_mlready_product_features import run_mlready_product_features_validation
    return run_mlready_product_features_validation(execution_date=execution_date)

def validate_mlready_user_features_wrapper(execution_date, **kwargs):
    from src.data_validation.validate_mlready_user_features import run_mlready_user_features_validation
    return run_mlready_user_features_validation(execution_date=execution_date)

def validate_mlready_product_review_stats_wrapper(execution_date, **kwargs):
    from src.data_validation.validate_mlready_product_review_stats import run_mlready_product_review_stats_validation
    return run_mlready_product_review_stats_validation(execution_date=execution_date)

def validate_mlready_user_product_interactions_wrapper(execution_date, **kwargs):
    from src.data_validation.validate_mlready_user_product_interactions import run_mlready_user_product_interactions_validation
    return run_mlready_user_product_interactions_validation(execution_date=execution_date)

def load_to_pgvector_wrapper(ingest_dt, **kwargs):
    from src.embeddings.step3_load_reviews_embeddings_to_pgvector import run_load_review_embeddings_to_pgvector
    return run_load_review_embeddings_to_pgvector(ingest_dt=ingest_dt)

def validate_pgvector_wrapper(**kwargs):
    from src.data_validation.validate_review_embeddings import validate_review_embeddings
    return validate_review_embeddings()


with DAG(
    dag_id="recommender_system_pipeline_prod",
    description="Production orchestration DAG for Project 3",
    default_args=default_args,
    start_date=datetime(2026, 3, 23),
    schedule=None,
    catchup=False,
    tags=["project3", "recommender", "pipeline", "prod", "emr-serverless", "ecr-image"],
) as dag:

    start = EmptyOperator(task_id="start")

    # PHASE 1: Products Ingestion and Transformation
    ingest_products = PythonOperator(
        task_id="ingest_products_to_s3_raw",
        python_callable=ingest_products_wrapper,
    )

    transform_products = EmrServerlessStartJobOperator(
        task_id="transform_raw_products_to_staging",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        aws_conn_id=AWS_CONN_ID,
        job_driver={
            "sparkSubmit": {
                "entryPoint": PRODUCTS_SCRIPT_S3_URI,
                "entryPointArguments": [build_manifest_key(entity="products", ingest_dt="{{ ds }}")],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}",
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

    validate_products = PythonOperator(
        task_id="validate_staging_products",
        python_callable=validate_staging_products_wrapper,
        op_kwargs={"ingest_dt": "{{ ds }}"},
    )

    # PHASE 1: Reviews Ingestion and Transformation
    ingest_reviews = PythonOperator(
        task_id="ingest_reviews_to_s3_raw",
        python_callable=ingest_reviews_wrapper,
    )

    transform_reviews = EmrServerlessStartJobOperator(
        task_id="transform_raw_reviews_to_staging",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        aws_conn_id=AWS_CONN_ID,
        job_driver={
            "sparkSubmit": {
                "entryPoint": REVIEWS_SCRIPT_S3_URI,
                "entryPointArguments": [build_manifest_key(entity="reviews", ingest_dt="{{ ds }}")],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}",
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

    validate_reviews = PythonOperator(
        task_id="validate_staging_reviews",
        python_callable=validate_staging_reviews_wrapper,
        op_kwargs={"ingest_dt": "{{ ds }}"},
    )

    # PHASE 2-5: Building ML-Ready Feature Sets
    build_mlready_product_features = EmrServerlessStartJobOperator(
        task_id="build_mlready_product_features",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": MLREADY_PRODUCTS_SCRIPT,
                "entryPointArguments": ["--input-path", f"s3://{S3_BUCKET}/staging/products/ingest_dt={{{{ ds }}}}/", "--execution-date", "{{ ds }}"],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}",
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

    # (Validation tasks for MLReady features remain as PythonOperators)
    validate_mlready_product_features = PythonOperator(
        task_id="validate_mlready_product_features",
        python_callable=validate_mlready_product_features_wrapper,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    build_mlready_user_features = EmrServerlessStartJobOperator(
        task_id="build_mlready_user_features",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": MLREADY_USERS_SCRIPT,
                "entryPointArguments": ["--input-path", f"s3://{S3_BUCKET}/staging/reviews/ingest_dt={{{{ ds }}}}/", "--execution-date", "{{ ds }}"],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}",
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

    validate_mlready_user_features = PythonOperator(
        task_id="validate_mlready_user_features",
        python_callable=validate_mlready_user_features_wrapper,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    build_mlready_product_review_stats = EmrServerlessStartJobOperator(
        task_id="build_mlready_product_review_stats",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": MLREADY_STATS_SCRIPT,
                "entryPointArguments": ["--input-path", f"s3://{S3_BUCKET}/staging/reviews/ingest_dt={{{{ ds }}}}/", "--execution-date", "{{ ds }}"],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}",
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

    validate_mlready_product_review_stats = PythonOperator(
        task_id="validate_mlready_product_review_stats",
        python_callable=validate_mlready_product_review_stats_wrapper,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    build_mlready_user_product_interactions = EmrServerlessStartJobOperator(
        task_id="build_mlready_user_product_interactions",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": MLREADY_INTERACTIONS_SCRIPT,
                "entryPointArguments": ["--execution-date", "{{ ds }}"],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}",
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

    validate_mlready_user_product_interactions = PythonOperator(
        task_id="validate_mlready_user_product_interactions",
        python_callable=validate_mlready_user_product_interactions_wrapper,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    # PHASE 6: Embedding Pipeline - CORE ML WORKLOAD
    prepare_and_filter_reviews_for_embeddings = EmrServerlessStartJobOperator(
        task_id="prepare_and_filter_reviews_for_embeddings",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": PREPARE_REVIEWS_FOR_EMBEDDINGS_SCRIPT,
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}",
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

    generate_review_embeddings = EmrServerlessStartJobOperator(
        task_id="generate_review_embeddings",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        wait_for_completion=True,
        job_driver={
            "sparkSubmit": {
                "entryPoint": GENERATE_REVIEW_EMBEDDINGS_SCRIPT,
                "sparkSubmitParameters": (
                    f"--py-files {PY_FILES_S3_URI} "
                    # Optimized resource allocation for ML models (SentenceTransformers/Torch)
                    "--conf spark.emr-serverless.driver.cpu=4vCPU "
                    "--conf spark.emr-serverless.driver.memory=16G "
                    "--conf spark.emr-serverless.executor.cpu=4vCPU "
                    "--conf spark.emr-serverless.executor.memory=16G "
                    "--conf spark.emr-serverless.executor.disk=100G "
                    "--conf spark.executor.instances=2 "
                    "--conf spark.dynamicAllocation.enabled=false "
                    "--conf spark.sql.execution.arrow.pyspark.enabled=true"
                ),
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

    # Final Local Tasks: Database Load and Validation
    load_review_embeddings_to_pgvector = PythonOperator(
        task_id="load_review_embeddings_to_pgvector",
        python_callable=load_to_pgvector_wrapper,
        op_kwargs={"ingest_dt": "{{ ds }}"},
    )

    validate_pgvector_review_embeddings = PythonOperator(
        task_id="validate_pgvector_review_embeddings",
        python_callable=validate_pgvector_wrapper,
    )

    end = EmptyOperator(task_id="end")

    # Dependency Graph (Sequential execution as requested to manage EMR quota)
    start >> ingest_products >> transform_products >> validate_products >> ingest_reviews >> transform_reviews >> validate_reviews
    validate_reviews >> build_mlready_product_features >> validate_mlready_product_features
    validate_mlready_product_features >> build_mlready_user_features >> validate_mlready_user_features
    validate_mlready_user_features >> build_mlready_product_review_stats >> validate_mlready_product_review_stats
    validate_mlready_product_review_stats >> build_mlready_user_product_interactions >> validate_mlready_user_product_interactions
    validate_mlready_user_product_interactions >> prepare_and_filter_reviews_for_embeddings >> generate_review_embeddings >> load_review_embeddings_to_pgvector >> validate_pgvector_review_embeddings >> end