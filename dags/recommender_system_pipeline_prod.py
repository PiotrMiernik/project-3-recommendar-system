# This DAG orchestrates the production version of the recommender system pipeline:
# - First it runs two parallel ingestion tasks:
#   PostgreSQL products_raw -> S3 Raw and MongoDB reviews_raw -> S3 Raw
# - Then it starts two Spark transformation jobs on EMR Serverless:
#   S3 Raw -> S3 Staging for products and reviews
# - After each EMR job finishes, it runs local Great Expectations validation
#   against the corresponding staging partition

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor

from src.common.s3_utils import build_manifest_key
from src.data_validation.validate_staging_products import run_staging_products_validation
from src.data_validation.validate_staging_reviews import run_staging_reviews_validation
from src.ingestion.ingest_products_to_s3_raw import main as ingest_products_main
from src.ingestion.ingest_reviews_to_s3_raw import main as ingest_reviews_main


default_args = {
    "owner": "piotr",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

EMR_SERVERLESS_APPLICATION_ID = Variable.get("emr_serverless_application_id")
EMR_SERVERLESS_EXECUTION_ROLE_ARN = Variable.get("emr_serverless_runtime_role_arn")

AWS_CONN_ID = "aws_default"
S3_BUCKET = "project-3-recommender-system"

PRODUCTS_SCRIPT_S3_URI = f"s3://{S3_BUCKET}/jobs/transform_raw_products_to_staging.py"
REVIEWS_SCRIPT_S3_URI = f"s3://{S3_BUCKET}/jobs/transform_raw_reviews_to_staging.py"
PY_FILES_S3_URI = f"s3://{S3_BUCKET}/jobs/src_package.zip"
EMR_LOGS_S3_URI = f"s3://{S3_BUCKET}/logs/emr-serverless/"

MLREADY_PRODUCTS_SCRIPT = f"s3://{S3_BUCKET}/jobs/build_mlready_product_features.py"
MLREADY_USERS_SCRIPT = f"s3://{S3_BUCKET}/jobs/build_mlready_user_features.py"
MLREADY_STATS_SCRIPT = f"s3://{S3_BUCKET}/jobs/build_mlready_product_review_stats.py"
MLREADY_INTERACTIONS_SCRIPT = f"s3://{S3_BUCKET}/jobs/build_mlready_user_product_interactions.py"

def run_validate_products_task(**context):
    ingest_dt = context["ds"]
    run_staging_products_validation(ingest_dt=ingest_dt)


def run_validate_reviews_task(**context):
    ingest_dt = context["ds"]
    run_staging_reviews_validation(ingest_dt=ingest_dt)

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
                "spark.executorEnv.AWS_REGION": "eu-central-1",
                "spark.executorEnv.S3_BUCKET": S3_BUCKET,
                "spark.executorEnv.S3_RAW_PREFIX": "raw/",
                "spark.executorEnv.S3_STAGING_PREFIX": "staging/",
                "spark.executorEnv.S3_MLREADY_PREFIX": "mlready/",
            },
        }
    ],
}

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

    ingest_products = PythonOperator(
        task_id="ingest_products_to_s3_raw",
        python_callable=ingest_products_main,
    )

    ingest_reviews = PythonOperator(
        task_id="ingest_reviews_to_s3_raw",
        python_callable=ingest_reviews_main,
    )

    transform_products = EmrServerlessStartJobOperator(
        task_id="transform_raw_products_to_staging",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=False,
        job_driver={
            "sparkSubmit": {
                "entryPoint": PRODUCTS_SCRIPT_S3_URI,
                "entryPointArguments": [
                    build_manifest_key(entity="products", ingest_dt="{{ ds }}"),
                ],
                "sparkSubmitParameters": (
                    f"--py-files {PY_FILES_S3_URI} "
                ),
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

    wait_for_products_transform = EmrServerlessJobSensor(
        task_id="wait_for_transform_raw_products_to_staging",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        job_run_id=transform_products.output,
        aws_conn_id=AWS_CONN_ID,
    )

    transform_reviews = EmrServerlessStartJobOperator(
        task_id="transform_raw_reviews_to_staging",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=False,
        job_driver={
            "sparkSubmit": {
                "entryPoint": REVIEWS_SCRIPT_S3_URI,
                "entryPointArguments": [
                    build_manifest_key(entity="reviews", ingest_dt="{{ ds }}"),
                ],
                "sparkSubmitParameters": (
                    f"--py-files {PY_FILES_S3_URI} "
                ),
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

    wait_for_reviews_transform = EmrServerlessJobSensor(
        task_id="wait_for_transform_raw_reviews_to_staging",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        job_run_id=transform_reviews.output,
        aws_conn_id=AWS_CONN_ID,
    )

    validate_products = PythonOperator(
        task_id="validate_staging_products",
        python_callable=run_validate_products_task,
    )

    validate_reviews = PythonOperator(
        task_id="validate_staging_reviews",
        python_callable=run_validate_reviews_task,
    )

    # 1. Product Features
    build_gold_products = EmrServerlessStartJobOperator(
        task_id="build_mlready_product_features",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": MLREADY_PRODUCTS_SCRIPT,
                "entryPointArguments": [
                    "--input-path", f"s3://{S3_BUCKET}/staging/products/ingest_dt={{{{ ds }}}}/",
                    "--execution-date", "{{ ds }}"
                ],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}"
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

    # 2. User Features
    build_gold_users = EmrServerlessStartJobOperator(
        task_id="build_mlready_user_features",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": MLREADY_USERS_SCRIPT,
                "entryPointArguments": [
                    "--input-path", f"s3://{S3_BUCKET}/staging/reviews/ingest_dt={{{{ ds }}}}/",
                    "--execution-date", "{{ ds }}"
                ],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}"
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

    # 3. Product Review Stats
    build_gold_stats = EmrServerlessStartJobOperator(
        task_id="build_mlready_product_review_stats",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": MLREADY_STATS_SCRIPT,
                "entryPointArguments": [
                    "--input-path", f"s3://{S3_BUCKET}/staging/reviews/ingest_dt={{{{ ds }}}}/",
                    "--execution-date", "{{ ds }}"
                ],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}"
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

    # 4. Master Interactions Table (The Big Join)
    build_gold_interactions = EmrServerlessStartJobOperator(
        task_id="build_mlready_user_product_interactions",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_SERVERLESS_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": MLREADY_INTERACTIONS_SCRIPT,
                "entryPointArguments": [
                    "--execution-date", "{{ ds }}"
                ],
                "sparkSubmitParameters": f"--py-files {PY_FILES_S3_URI}"
            }
        },
        configuration_overrides=COMMON_CONFIGURATION_OVERRIDES,
    )

    end = EmptyOperator(task_id="end")

# --- FULLY SEQUENTIAL DATA PIPELINE DEPENDENCIES ---
    
    # PHASE 1: Data Ingestion
    # Ingestion runs in parallel as these are standard Python tasks, 
    # not consuming EMR Serverless capacity.
    start >> [ingest_products, ingest_reviews]

    # PHASE 2: Staging Layer (Sequential EMR Execution)
    # We process products first to avoid resource contention on EMR.
    ingest_products >> transform_products >> wait_for_products_transform >> validate_products
    
    # Review transformation starts only after Product Staging is validated.
    validate_products >> transform_reviews >> wait_for_reviews_transform >> validate_reviews

    # PHASE 3: Gold Layer (Feature Engineering)
    # We build smaller feature tables sequentially to stay within EMR resource limits.
    # 3.1. Create Product Metadata features
    validate_reviews >> build_gold_products
    
    # 3.2. Create User Behavioral profiles
    build_gold_products >> build_gold_users
    
    # 3.3. Calculate Product Review statistics (Social Proof)
    build_gold_users >> build_gold_stats
    
    # PHASE 4: Master Feature Table (The Final Join)
    # Once all dimension tables are updated in Iceberg, trigger the final join.
    build_gold_stats >> build_gold_interactions >> end