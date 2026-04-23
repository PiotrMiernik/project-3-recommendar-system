import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import psycopg2
from dotenv import load_dotenv

from src.common.logging import get_logger

logger = get_logger(__name__)

SCHEMA_NAME = "vector"
TABLE_NAME = "review_embeddings"
EXPECTED_VECTOR_DIM = 384


def load_settings() -> Dict[str, Any]:
    """
    Load local settings from config/.env.

    This validation script is intended to run as a local Airflow task,
    so it loads PostgreSQL connection settings directly from the .env file.
    """
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / "config" / ".env"
    load_dotenv(env_path)

    return {
        "rds_host": os.getenv("RDS_HOST"),
        "rds_port": int(os.getenv("RDS_PORT", "5432")),
        "rds_dbname": os.getenv("RDS_DBNAME"),
        "rds_user": os.getenv("RDS_USER"),
        "rds_password": os.getenv("RDS_PASSWORD"),
    }


def get_db_connection(settings: Dict[str, Any]):
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


def run_scalar_query(conn, query: str) -> int:
    """
    Execute a SQL query that returns a single scalar value.
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()

    if result is None:
        raise ValueError("Scalar query returned no result.")

    return int(result[0])


def build_validation_results(conn) -> Dict[str, Any]:
    """
    Run all validation checks for vector.review_embeddings
    and return results as a dictionary.
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

    checks: List[Dict[str, Any]] = [
        {
            "check_name": "table_not_empty",
            "metric_value": total_count,
            "expected_condition": "count > 0",
            "passed": total_count > 0,
        },
        {
            "check_name": "no_nulls_in_required_columns",
            "metric_value": null_count,
            "expected_condition": "count = 0",
            "passed": null_count == 0,
        },
        {
            "check_name": "no_duplicate_review_id",
            "metric_value": duplicate_review_id_count,
            "expected_condition": "count = 0",
            "passed": duplicate_review_id_count == 0,
        },
        {
            "check_name": "all_embeddings_have_expected_dimension",
            "metric_value": invalid_vector_dim_count,
            "expected_condition": f"count = 0 for vector_dims(embedding) != {EXPECTED_VECTOR_DIM}",
            "passed": invalid_vector_dim_count == 0,
        },
    ]

    overall_status = "success" if all(check["passed"] for check in checks) else "failed"

    return {
        "execution_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "database_name": conn.info.dbname,
        "schema_name": SCHEMA_NAME,
        "table_name": TABLE_NAME,
        "expected_vector_dimension": EXPECTED_VECTOR_DIM,
        "overall_status": overall_status,
        "checks": checks,
    }


def save_results_to_json(results: Dict[str, Any]) -> Path:
    """
    Save validation results to a JSON file.

    File name pattern:
    validate_review_embeddings_<execution_timestamp>.json
    """
    project_root = Path(__file__).resolve().parents[2]
    output_dir = project_root / "src" / "data_validation" / "validation_results"
    output_dir.mkdir(parents=True, exist_ok=True)

    execution_dt = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"validate_review_embeddings_{execution_dt}.json"

    with open(output_path, "w", encoding="utf-8") as json_file:
        json.dump(results, json_file, indent=4)

    return output_path


def log_validation_summary(results: Dict[str, Any]) -> None:
    """
    Log a readable validation summary for Airflow task logs.
    """
    logger.info(
        f"Validation status for {results['schema_name']}.{results['table_name']}: "
        f"{results['overall_status']}"
    )

    for check in results["checks"]:
        logger.info(
            f"Check: {check['check_name']} | "
            f"metric_value={check['metric_value']} | "
            f"passed={check['passed']}"
        )


def validate_review_embeddings() -> None:
    """
    Main entrypoint for validation of pgvector review embeddings table.

    The script:
    - connects to PostgreSQL,
    - runs validation checks,
    - saves results to JSON,
    - raises an error if validation fails.
    """
    settings = load_settings()
    conn = None

    try:
        conn = get_db_connection(settings)

        results = build_validation_results(conn)
        output_path = save_results_to_json(results)

        log_validation_summary(results)
        logger.info(f"Validation results saved to: {output_path}")

        if results["overall_status"] != "success":
            raise ValueError(
                f"Validation failed for {SCHEMA_NAME}.{TABLE_NAME}. "
                f"See JSON results file: {output_path}"
            )

        logger.info("Review embeddings validation finished successfully.")

    except Exception as exc:
        logger.exception(f"Review embeddings validation failed: {exc}")
        raise

    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    validate_review_embeddings()