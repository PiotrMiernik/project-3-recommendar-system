import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import psycopg2

from src.common.config import load_settings
from src.common.logging import get_logger


logger = get_logger(__name__)

SCHEMA_NAME = "vector"
TABLE_NAME = "review_embeddings"
EXPECTED_VECTOR_DIM = 384


# Database connection
def get_db_connection(settings: dict):
    """
    Create and return a PostgreSQL connection.
    """
    return psycopg2.connect(
        host=settings["rds_host"],
        port=settings["rds_port"],
        dbname=settings["rds_dbname"],
        user=settings["rds_user"],
        password=settings["rds_password"],
    )


# Validation helpers
def run_scalar_query(conn, query: str) -> int:
    """
    Execute a SQL query that returns a single scalar integer value.
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()

    if result is None:
        raise ValueError("Scalar query returned no result.")

    return int(result[0])


def build_checks(conn) -> List[Dict[str, Any]]:
    """
    Run all validation checks for vector.review_embeddings.
    Returns a list of normalized check results.
    """
    total_count = run_scalar_query(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {SCHEMA_NAME}.{TABLE_NAME}
        """
    )

    null_count = run_scalar_query(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {SCHEMA_NAME}.{TABLE_NAME}
        WHERE review_id IS NULL
           OR embedding IS NULL
           OR model_version IS NULL
           OR text_hash IS NULL
        """
    )

    duplicate_review_id_count = run_scalar_query(
        conn,
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT review_id
            FROM {SCHEMA_NAME}.{TABLE_NAME}
            GROUP BY review_id
            HAVING COUNT(*) > 1
        ) AS duplicates
        """
    )

    invalid_vector_dim_count = run_scalar_query(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {SCHEMA_NAME}.{TABLE_NAME}
        WHERE vector_dims(embedding) != {EXPECTED_VECTOR_DIM}
        """
    )

    return [
        {
            "check_name": "table_not_empty",
            "metric_value": total_count,
            "expected_condition": "count > 0",
            "success": total_count > 0,
        },
        {
            "check_name": "no_nulls_in_required_columns",
            "metric_value": null_count,
            "expected_condition": "count = 0",
            "success": null_count == 0,
        },
        {
            "check_name": "no_duplicate_review_id",
            "metric_value": duplicate_review_id_count,
            "expected_condition": "count = 0",
            "success": duplicate_review_id_count == 0,
        },
        {
            "check_name": "all_embeddings_have_expected_dimension",
            "metric_value": invalid_vector_dim_count,
            "expected_condition": f"count = 0 for vector_dims(embedding) != {EXPECTED_VECTOR_DIM}",
            "success": invalid_vector_dim_count == 0,
        },
    ]


def run_validation(conn):
    """
    Execute all validation checks and return overall success flag
    together with detailed results.
    """
    checks = build_checks(conn)
    overall_success = all(check["success"] for check in checks)

    for check in checks:
        logger.info(
            f"{check['check_name']} -> success={check['success']} | "
            f"metric_value={check['metric_value']}"
        )

    return overall_success, checks


# Save results
def save_results(
    output_dir: str,
    execution_date: str,
    success: bool,
    results: list,
):
    """
    Save validation results to a local JSON file.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    output_path = Path(output_dir) / f"validate_review_embeddings_{execution_date}.json"

    payload = {
        "schema_name": SCHEMA_NAME,
        "table_name": TABLE_NAME,
        "execution_date": execution_date,
        "expected_vector_dimension": EXPECTED_VECTOR_DIM,
        "success": success,
        "checks": results,
    }

    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    return output_path


# Airflow entrypoint
def validate_review_embeddings(
    output_dir: str = "src/data_validation/validation_results",
) -> None:
    """
    Main entrypoint for Airflow.
    Runs SQL-based validation for vector.review_embeddings.
    """
    settings = load_settings()
    execution_date = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    logger.info(f"Starting validation for {SCHEMA_NAME}.{TABLE_NAME}")
    logger.info(f"Execution date: {execution_date}")

    conn = None

    try:
        conn = get_db_connection(settings)

        success, results = run_validation(conn)

        result_path = save_results(
            output_dir=output_dir,
            execution_date=execution_date,
            success=success,
            results=results,
        )

        logger.info(f"Validation results saved: {result_path}")
        logger.info(f"Validation success: {success}")

        if not success:
            raise ValueError(f"Validation failed for {SCHEMA_NAME}.{TABLE_NAME}")

    except Exception as exc:
        logger.exception(f"Validation failed for {SCHEMA_NAME}.{TABLE_NAME}: {exc}")
        raise

    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    validate_review_embeddings()