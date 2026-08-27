"""
Environment-aware logger with S3 upload capability.

Log level mapping:
  dev / sit   → DEBUG
  uat / prod  → INFO
  fallback    → config/.env.{environment}
"""
import logging
import os
import sys
import atexit
import shutil
import tempfile
from pathlib import Path
from typing import Optional

_LOG_LEVEL_MAP = {
    "DEBUG":   logging.DEBUG,
    "INFO":    logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR":   logging.ERROR,
}

_LOCAL_LOG_FILE = None
_S3_LOG_PATH = None


def _read_env_log_level(environment: str, repo_root: Optional[str] = None) -> int:
    """
    Reads LOG_LEVEL from config/.env.<environment>.
    Searches for the config/ directory relative to repo_root (if given),
    or by walking up from this file's location.
    """
    if repo_root:
        env_file = Path(repo_root) / "config" / f".env.{environment}"
    else:
        # Walk up from src/ingestion/utils/ → find config/ alongside src/
        here = Path(__file__).resolve().parent          # utils/
        for _ in range(5):                              # walk up max 5 levels
            candidate = here / "config" / f".env.{environment}"
            if candidate.exists():
                env_file = candidate
                break
            here = here.parent
        else:
            return logging.INFO                         # fallback

    try:
        with open(env_file, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line.startswith("LOG_LEVEL"):
                    _, _, value = line.partition("=")
                    return _LOG_LEVEL_MAP.get(value.strip().upper(), logging.INFO)
    except Exception:
        pass
    return logging.INFO


def get_logger(
    name: str = "ingestion_framework",
    environment: str = "dev",
    repo_root: Optional[str] = None,
) -> logging.Logger:
    """
    Returns a configured logger with log level determined by environment.

    Parameters
    ----------
    name        : logger name (default: 'ingestion_framework')
    environment : 'dev' | 'sit' | 'uat' | 'prod' — controls log level
    repo_root   : absolute path to the repository root (optional)
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Resolve log level dynamically based on environment
    env_lower = str(environment).lower().strip()
    if env_lower in ("dev", "sit"):
        level = logging.DEBUG
    elif env_lower in ("uat", "prod"):
        level = logging.INFO
    else:
        level = _read_env_log_level(environment, repo_root)

    logger.setLevel(level)
    return logger


def _upload_on_exit():
    """Uploads the local log file to S3 or DBFS/Volume on program exit."""
    global _LOCAL_LOG_FILE, _S3_LOG_PATH
    if not _LOCAL_LOG_FILE or not _S3_LOG_PATH:
        return

    try:
        # Flush all logger handlers to ensure logs are fully written to disk
        for handler in logging.getLogger().handlers:
            handler.flush()
        for name in logging.root.manager.loggerDict:
            logger = logging.getLogger(name)
            for handler in logger.handlers:
                handler.flush()

        local_file = _LOCAL_LOG_FILE
        destination_path = _S3_LOG_PATH

        if not os.path.exists(local_file) or os.path.getsize(local_file) == 0:
            return

        if destination_path.startswith("s3://"):
            import boto3
            # Parse s3://bucket/key
            path_parts = destination_path[5:].split("/", 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ""

            if not key or key.endswith("/"):
                filename = os.path.basename(local_file)
                key = f"{key}{filename}"

            s3 = boto3.client("s3")
            s3.upload_file(local_file, bucket, key)
            print(f"[Logger] Successfully uploaded logs to S3: s3://{bucket}/{key}")
        else:
            # UC Volume or DBFS or local filesystem path
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            shutil.copy(local_file, destination_path)
            print(f"[Logger] Successfully copied logs to: {destination_path}")

    except Exception as e:
        print(f"[Logger] Failed to upload logs to destination '{_S3_LOG_PATH}': {e}")


def configure_s3_logging(s3_log_path: str, logger_name: str = "ingestion_framework") -> None:
    """
    Configures a FileHandler that writes logs to a local file, and registers an
    atexit hook to upload the log file to the specified S3/DBFS path on termination.
    """
    global _LOCAL_LOG_FILE, _S3_LOG_PATH
    _S3_LOG_PATH = s3_log_path

    logger = logging.getLogger(logger_name)

    # Create a unique local log file in the temp directory
    temp_dir = tempfile.gettempdir()
    _LOCAL_LOG_FILE = os.path.join(temp_dir, f"{logger_name}_execution.log")

    # Check if a FileHandler already points to this file to avoid duplicates
    for h in logger.handlers:
        if isinstance(h, logging.FileHandler) and h.baseFilename == _LOCAL_LOG_FILE:
            return

    file_handler = logging.FileHandler(_LOCAL_LOG_FILE, mode="w", encoding="utf-8")
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Register the upload function to trigger on termination
    atexit.register(_upload_on_exit)
    logger.info(f"[Logger] Configured S3/Volume logging. Logs will be uploaded to: {s3_log_path}")
