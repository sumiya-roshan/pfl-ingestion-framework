"""
Environment-aware logger using python-dotenv.

Log level is driven by  config/.env.{environment}  where environment is one of:
  dev  → DEBUG    (config/.env.dev  : LOG_LEVEL=DEBUG)
  sit  → DEBUG    (config/.env.sit  : LOG_LEVEL=DEBUG)
  uat  → INFO     (config/.env.uat  : LOG_LEVEL=INFO)
  prod → INFO     (config/.env.prod : LOG_LEVEL=INFO)

The environment value is passed from the notebook widget into IngestionOrchestrator
which passes it to get_logger(). Falls back to INFO if the .env file is missing
or dotenv is not installed.
"""

import atexit
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path

_LOG_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

_LOCAL_LOG_FILE: str | None = None
_S3_LOG_PATH: str | None = None


def _find_env_file(environment: str, repo_root: str | None = None) -> Path | None:
    """
    Locate  config/.env.<environment>  relative to the repo root.
    Walks up from this file's location if repo_root is not given.
    """
    filename = f".env.{environment.lower().strip()}"

    if repo_root:
        candidate = Path(repo_root) / "config" / filename
        return candidate if candidate.exists() else None

    # Walk up from src/ingestion/utils/ looking for a config/ sibling of src/
    here = Path(__file__).resolve().parent  # utils/
    for _ in range(6):  # walk up at most 6 levels
        candidate = here / "config" / filename
        if candidate.exists():
            return candidate
        here = here.parent
    return None


def _read_env_log_level(environment: str, repo_root: str | None = None) -> int:
    """
    Load the .env.<environment> file with dotenv and return the numeric log level.
    Falls back to INFO on any error (missing file, dotenv not installed, etc.).
    """
    env_file = _find_env_file(environment, repo_root)
    if env_file is None:
        return logging.INFO

    try:
        from dotenv import dotenv_values  # python-dotenv

        values = dotenv_values(env_file)
        raw = values.get("LOG_LEVEL", "").strip().upper()
        return _LOG_LEVEL_MAP.get(raw, logging.INFO)
    except ImportError:
        # dotenv not installed — parse manually as a last resort
        try:
            with open(env_file, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line.startswith("LOG_LEVEL"):
                        _, _, value = line.partition("=")
                        return _LOG_LEVEL_MAP.get(value.strip().upper(), logging.INFO)
        except OSError as exc:
            print(f"[Logger] Could not read '{env_file}' ({exc}) — defaulting to INFO.")
    except Exception as exc:
        print(f"[Logger] Failed to load '{env_file}' via dotenv ({exc}) — defaulting to INFO.")

    return logging.INFO


def get_logger(
    name: str = "ingestion_framework",
    environment: str = "dev",
    repo_root: str | None = None,
) -> logging.Logger:
    """
    Returns a configured logger whose level is read from
    config/.env.<environment> via python-dotenv.

    Parameters
    ----------
    name        : logger name  (default: 'ingestion_framework')
    environment : 'dev' | 'sit' | 'uat' | 'prod' — selects the .env file
    repo_root   : absolute path to the repository root (auto-detected if None)
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

    level = _read_env_log_level(environment, repo_root)
    logger.setLevel(level)
    return logger


# ─── S3 / Volume log-upload support ──────────────────────────────────────────

_DBUTILS = None  # injected from the notebook via configure_s3_logging(dbutils=...)


def _upload_on_exit() -> None:
    """Flush log handlers and upload the local log file to S3 / Volume.
    Idempotent — safe to call multiple times; only uploads once.
    Uses dbutils.fs.cp() (same IAM creds as Spark) when available.
    """
    global _LOCAL_LOG_FILE, _S3_LOG_PATH
    if not _LOCAL_LOG_FILE or not _S3_LOG_PATH:
        return

    # Capture and clear so any second call (e.g. atexit) is a no-op
    local_file = _LOCAL_LOG_FILE
    dest = _S3_LOG_PATH
    dbutils = _DBUTILS
    _LOCAL_LOG_FILE = None
    _S3_LOG_PATH = None

    # Flush every known handler so nothing is lost before upload
    for lname in [""] + list(logging.root.manager.loggerDict.keys()):
        lgr = logging.getLogger(lname)
        for h in lgr.handlers:
            try:
                h.flush()
            except Exception as exc:
                print(f"[Logger] Failed to flush handler for '{lname}': {exc}")

    if not os.path.exists(local_file) or os.path.getsize(local_file) == 0:
        print("[Logger] Log file is empty — skipping upload.")
        return

    try:
        if dbutils is not None:
            # Read log content with Python (always allowed on Serverless),
            # then write via dbutils.fs.put() which uses the cluster's IAM role.
            # NOTE: dbutils.fs.cp("file:...") is blocked on Serverless clusters.
            with open(local_file, "r", encoding="utf-8") as fh:
                log_content = fh.read()
            dbutils.fs.put(dest, log_content, overwrite=True)
            print(f"[Logger] Uploaded logs -> {dest}")
        elif not dest.startswith("s3://"):
            # Local / UC Volume / DBFS — plain file copy
            dest_dir = os.path.dirname(dest)
            if dest_dir:
                os.makedirs(dest_dir, exist_ok=True)
            shutil.copy(local_file, dest)
            print(f"[Logger] Copied logs  -> {dest}")
        else:
            # No dbutils and S3 path — last resort: boto3
            import boto3

            path_parts = dest[5:].split("/", 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ""
            if not key or key.endswith("/"):
                key = f"{key}{os.path.basename(local_file)}"
            boto3.client("s3").upload_file(local_file, bucket, key)
            print(f"[Logger] Uploaded logs -> s3://{bucket}/{key}")
    except Exception as exc:
        print(f"[Logger] Failed to upload logs to '{dest}': {exc}")


def configure_s3_logging(
    s3_log_path: str,
    logger_name: str = "ingestion_framework",
    dbutils=None,
) -> None:
    """
    Attach a FileHandler that writes logs to a local temp file during execution.
    Call _upload_on_exit() explicitly at the end of the notebook to upload.

    Parameters
    ----------
    s3_log_path  : destination — 's3://bucket/prefix/file.log' or a Volume path
    logger_name  : logger name to attach the FileHandler to
    dbutils      : pass dbutils from your notebook so upload uses dbutils.fs.cp()
                   (same IAM credentials as Spark — works on Serverless clusters)
    """
    global _LOCAL_LOG_FILE, _S3_LOG_PATH, _DBUTILS
    _S3_LOG_PATH = s3_log_path
    _DBUTILS = dbutils

    _LOCAL_LOG_FILE = os.path.join(
        tempfile.gettempdir(),
        f"{logger_name}_execution.log",
    )

    lgr = logging.getLogger(logger_name)

    # Avoid adding a duplicate FileHandler
    for h in lgr.handlers:
        if isinstance(h, logging.FileHandler) and h.baseFilename == os.path.abspath(
            _LOCAL_LOG_FILE
        ):
            return

    fh = logging.FileHandler(_LOCAL_LOG_FILE, mode="w", encoding="utf-8")
    fh.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    lgr.addHandler(fh)

    atexit.register(_upload_on_exit)
    lgr.info(f"[Logger] Log file will be uploaded to: {s3_log_path}")
