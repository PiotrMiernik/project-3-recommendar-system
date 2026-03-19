# This module provides a unified logger configuration for the project,
# ensuring consistent logging across ingestion scripts, Airflow, and other components.

import logging
import sys

# Creates and returns a configured logger instance.
# Ensures that multiple handlers are not added on repeated imports.
def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
            )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        
    return logger