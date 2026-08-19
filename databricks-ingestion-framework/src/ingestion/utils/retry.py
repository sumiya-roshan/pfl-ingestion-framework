"""
Generic retry helper for source-connection operations (e.g. the initial
`select *` pull from a JDBC/Mongo/SFTP/S3 source). Not intended for writes
or transformations — those should fail fast rather than silently repeat.
"""
import time
from typing import Callable, Optional, TypeVar

T = TypeVar("T")


def retry_on_failure(
    func: Callable[[], T],
    max_retries: int = 0,
    retry_interval: int = 0,
    logger=None,
    description: str = "operation",
) -> T:
    """
    Calls func() and retries on exception up to max_retries times, sleeping
    retry_interval seconds between attempts. Re-raises the last exception
    once retries are exhausted.

    Parameters
    ----------
    func           : zero-arg callable to invoke (wrap the real call in a lambda)
    max_retries    : number of retries after the first attempt (0 = no retry)
    retry_interval : seconds to sleep between attempts
    logger         : optional logger for retry messages (falls back to print)
    description    : short label for log messages, e.g. "extract config_id=12"
    """
    attempt = 0
    while True:
        try:
            return func()
        except Exception as exc:
            if attempt >= max_retries:
                raise
            attempt += 1
            message = (
                f"{description} failed (attempt {attempt}/{max_retries}), "
                f"retrying in {retry_interval}s: {exc}"
            )
            if logger:
                logger.warning(message)
            else:
                print(message)
            if retry_interval > 0:
                time.sleep(retry_interval)
