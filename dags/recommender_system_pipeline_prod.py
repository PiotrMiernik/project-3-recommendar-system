# This DAG orchestrates the production version of the recommender system pipeline.
# All local Python tasks use "Lazy Imports" to prevent Airflow DAG parsing timeouts
# caused by heavy ML libraries (torch, sentence-transformers) or database drivers.
# The entire pipeline is executed sequentially to avoid EMR/Spark resource contention.
#
# Flow:
# 1. Ingest products to S3 Raw
# 2. Transform products Raw -> Staging on EMR Serverless
# 3. Validate staging products locally with Great Expectations
# 4. Ingest reviews to S3 Raw
# 5. Transform reviews Raw -> Staging on EMR Serverless
# 6. Validate staging reviews locally with Great Expectations
# 7. Build mlready product_features on EMR Serverless
# 8. Validate mlready product_features locally with Great Expectations
# 9. Build mlready user_features on EMR Serverless
# 10. Validate mlready user_features locally with Great Expectations
# 11. Build mlready product_review_stats on EMR Serverless
# 12. Validate mlready product_review_stats locally with Great Expectations
# 13. Build mlready user_product_interactions on EMR Serverless
# 14. Validate mlready user_product_interactions locally with Great Expectations
# 15. Prepare and filter reviews for embedding generation on EMR Serverless
# 16. Generate review embeddings on EMR Serverless
# 17. Load review embeddings into pgvector locally
# 18. Validate pgvector review embeddings locally with SQL-based checks

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

# We keep lightweight utility imports at the top
from src.common.s3_utils import build_manifest_key

default_args = {
    "owner": "piotr",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# IMPROVEMENT 1: Use Jinja templates for Airflow Variables.
# This prevents the Scheduler from hitting the Metadata DB every time it parses the file.
EMR_SERVERLESS_APPLICATION_ID = "{{ var.value.emr_serverless_application_id }}"
EMR_SERVERLESS_EXECUTION_ROLE_ARN = "{{ var.value.emr_serverless_runtime_role_arn }}"

AWS_CONN_ID = "aws_default"
S3_BUCKET = "project-3-recommender-system"

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
}

# IMPROVEMENT 2: Wrapper functions for Lazy Loading.
# These functions import logic only when the task is executed on the worker.

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
    description="Production orchestration DAG for the recommender system project",
    default_args=default_args,
    start_date=datetime(2026, 3, 23),
    schedule=None,
    catchup=False,
    tags=["project3", "recommender", "pipeline", "prod", "emr-serverless"],
) as dag:

    start = EmptyOperator(task_id="start")

    # PHASE 1: Products
    ingest_products = PythonOperator(
        task_id="ingest_products_to_s3_raw",
        python_callable=ingest_products_wrapper,
    )

    transform_products = EmrServerlessStartJobOperator(
        task_id="transform_raw_products_to_staging",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
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

    # PHASE 1: Reviews
    ingest_reviews = PythonOperator(
        task_id="ingest_reviews_to_s3_raw",
        python_callable=ingest_reviews_wrapper,
    )

    transform_reviews = EmrServerlessStartJobOperator(
        task_id="transform_raw_reviews_to_staging",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
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

    # PHASE 2-5: MLReady Features
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

    # PHASE 6: Embedding Pipeline
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
        job_driver={
            "sparkSubmit": {
                "entryPoint": GENERATE_REVIEW_EMBEDDINGS_SCRIPT,
                "sparkSubmitParameters": (
                    f"--py-files {PY_FILES_S3_URI} "
                    "--conf spark.archives=s3://project-3-recommender-system/artifacts/environment.tar.gz#environment "
                    "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python "
                    "--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python "
                    "--conf spark.sql.execution.arrow.pyspark.enabled=true"
                ),
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

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

    # Dependency Graph
    start >> ingest_products >> transform_products >> validate_products >> ingest_reviews >> transform_reviews >> validate_reviews
    validate_reviews >> build_mlready_product_features >> validate_mlready_product_features
    validate_mlready_product_features >> build_mlready_user_features >> validate_mlready_user_features
    validate_mlready_user_features >> build_mlready_product_review_stats >> validate_mlready_product_review_stats
    validate_mlready_product_review_stats >> build_mlready_user_product_interactions >> validate_mlready_user_product_interactions
    validate_mlready_user_product_interactions >> prepare_and_filter_reviews_for_embeddings >> generate_review_embeddings >> load_review_embeddings_to_pgvector >> validate_pgvector_review_embeddings >> end