"""
Lightweight logger wrapper. Swap out for your standard logging setup
(e.g. log4j via Spark JVM, or a centralized observability sink) as needed.
"""
import logging
import sys


def get_logger(name: str = "ingestion_framework") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
