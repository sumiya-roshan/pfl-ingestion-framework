"""
Environment-aware logger.

Log level is driven by  config/.env.{environment}  where environment is one of:
  dev  → DEBUG
  uat  → INFO
  prod → WARNING

The environment value is passed from the notebook widget into IngestionOrchestrator
which passes it to get_logger().  Falls back to INFO if the .env file is missing.
"""
import logging
import os
import sys
from pathlib import Path
from typing import Optional

_LOG_LEVEL_MAP = {
    "DEBUG":   logging.DEBUG,
    "INFO":    logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR":   logging.ERROR,
}


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
    Returns a configured logger.

    Parameters
    ----------
    name        : logger name (default: 'ingestion_framework')
    environment : 'dev' | 'uat' | 'prod'  — controls log level via .env file
    repo_root   : absolute path to the repository root (optional);
                  auto-detected if not provided
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
