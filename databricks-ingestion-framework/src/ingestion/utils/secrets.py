"""
Thin wrapper around Databricks secrets so the rest of the codebase never
touches dbutils directly (makes unit testing outside a notebook possible).

Credential convention
---------------------
config_source_system.secret_key_credentials stores a JSON payload in the
Databricks Secret Scope:

    {"username": "<value>", "password": "<value>"}

Use get_credentials(scope, key) to retrieve both values in one call.
For token-only sources (e.g. REST APIs) the JSON may omit "username".
"""
import json
import os
from typing import Optional, Tuple


class SecretResolver:

    def __init__(self, dbutils=None):
        """
        dbutils : pass in the notebook's ``dbutils`` object at runtime.
        When dbutils is None (e.g. local unit tests) the resolver falls back to
        environment variables named  ``<SCOPE>__<KEY>``  (upper-cased, hyphens
        replaced with underscores).
        """
        self._dbutils = dbutils

    # ── Low-level: fetch a single raw secret value ────────────────────────────

    def get(self, scope: str, key: str) -> str:
        """Return the raw string value of a Databricks secret."""
        if self._dbutils is not None:
            return self._dbutils.secrets.get(scope=scope, key=key)
        env_key = f"{scope}__{key}".upper().replace("-", "_")
        value: Optional[str] = os.environ.get(env_key)
        if value is None:
            raise ValueError(
                f"Secret not found for scope='{scope}' key='{key}' "
                f"(also checked env var '{env_key}')"
            )
        return value

    # ── High-level: parse JSON credential blob ────────────────────────────────

    def get_credentials(self, scope: str, key: str) -> Tuple[str, str]:
        """
        Retrieve and parse a JSON credential secret.

        The secret value must be a JSON object:
            {"username": "<value>", "password": "<value>"}

        Returns
        -------
        (username, password) : both as plain strings.

        Raises
        ------
        ValueError  if the secret is missing, not valid JSON, or lacks the
                    required keys.
        """
        raw = self.get(scope, key)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Secret scope='{scope}' key='{key}' must be a JSON string "
                f"(e.g. {{\"username\": \"x\", \"password\": \"y\"}}). "
                f"JSON parse error: {exc}"
            ) from exc

        username = payload.get("username", "")
        password = payload.get("password")

        if password is None:
            raise ValueError(
                f"Secret scope='{scope}' key='{key}' JSON must contain a 'password' field."
            )
        return username, password
