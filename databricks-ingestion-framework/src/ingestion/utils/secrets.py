# """
# Thin wrapper around Databricks secrets so the rest of the codebase never
# touches dbutils directly (makes unit testing outside a notebook possible).

# Credential convention
# ---------------------
# config_source_system.secret_key_credentials stores a JSON payload in the
# Databricks Secret Scope:

#     {"username": "<value>", "password": "<value>"}

# Use get_credentials(scope, key) to retrieve both values in one call.
# For token-only sources (e.g. REST APIs) the JSON may omit "username".
# """
# import json
# import os
# from typing import Optional, Tuple


# class SecretResolver:

#     def __init__(self, dbutils=None):
#         """
#         dbutils : pass in the notebook's ``dbutils`` object at runtime.
#         When dbutils is None (e.g. local unit tests) the resolver falls back to
#         environment variables named  ``<SCOPE>__<KEY>``  (upper-cased, hyphens
#         replaced with underscores).
#         """
#         self._dbutils = dbutils

#     # ── Low-level: fetch a single raw secret value ────────────────────────────

#     def get(self, scope: str, key: str) -> str:
#         """Return the raw string value of a Databricks secret."""
#         if self._dbutils is not None:
#             return self._dbutils.secrets.get(scope=scope, key=key)
#         env_key = f"{scope}__{key}".upper().replace("-", "_")
#         value: Optional[str] = os.environ.get(env_key)
#         if value is None:
#             raise ValueError(
#                 f"Secret not found for scope='{scope}' key='{key}' "
#                 f"(also checked env var '{env_key}')"
#             )
#         return value

#     # ── High-level: parse JSON credential blob ────────────────────────────────

#     def get_credentials(self, scope: str, key: str) -> Tuple[str, str]:
#         """
#         Retrieve and parse a JSON credential secret.

#         The secret value must be a JSON object:
#             {"username": "<value>", "password": "<value>"}

#         Returns
#         -------
#         (username, password) : both as plain strings.

#         Raises
#         ------
#         ValueError  if the secret is missing, not valid JSON, or lacks the
#                     required keys.
#         """
#         raw = self.get(scope, key)
#         try:
#             payload = json.loads(raw)
#         except json.JSONDecodeError as exc:
#             raise ValueError(
#                 f"Secret scope='{scope}' key='{key}' must be a JSON string "
#                 f"(e.g. {{\"username\": \"x\", \"password\": \"y\"}}). "
#                 f"JSON parse error: {exc}"
#             ) from exc

#         username = payload.get("user", "")
#         password = payload.get("password")

#         if password is None:
#             raise ValueError(
#                 f"Secret scope='{scope}' key='{key}' JSON must contain a 'password' field."
#             )
#         return username, password



"""
Thin wrapper around Databricks secrets so the rest of the codebase never
touches dbutils directly (makes unit testing outside a notebook possible).

Credential convention
---------------------
config_source_system.secret_key_credentials stores a JSON payload in the
Databricks Secret Scope:

    {"username": "<value>", "password": "<value>"}

Accepted username key aliases: "username" or "user" (both work).
Use get_credentials(scope, key) to retrieve both values in one call.
For token-only sources (e.g. REST APIs) the JSON may omit "username"/"user".

IMPORTANT — storing secrets from the CLI
-----------------------------------------
Use the interactive prompt to avoid shell quoting issues:
    databricks secrets put-secret <scope> <key>
    (then paste the JSON value when prompted)

Or on Linux/macOS only:
    databricks secrets put-secret <scope> <key> --string-value '{"username":"x","password":"y"}'
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
        """
        Return the raw string value of a secret.
        
        If scope is 'aws' or 'aws-secrets-manager', retrieves from AWS Secrets Manager.
        Otherwise, retrieves from Databricks Secret Scopes.
        """
        is_aws = scope.lower() in ("aws", "aws-secrets-manager", "aws_secrets_manager")

        if is_aws:
            # Check for local environment variable fallback first (helps local testing)
            env_key = f"{scope}__{key}".upper().replace("-", "_")
            if env_key in os.environ:
                return os.environ[env_key]
            
            try:
                import boto3
                client = boto3.client("secretsmanager")
                response = client.get_secret_value(SecretId=key)
                if "SecretString" in response:
                    return response["SecretString"]
                else:
                    import base64
                    return base64.b64decode(response["SecretBinary"]).decode("utf-8")
            except Exception as e:
                raise ValueError(
                    f"Failed to fetch secret '{key}' from AWS Secrets Manager. Error: {e}"
                )

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

        The secret value must be a JSON object with the following keys:
            {"username": "<value>", "password": "<value>"}

        Accepted username key aliases: "username" or "user" (either is fine).

        The method automatically strips surrounding whitespace and accidental
        wrapping quotes that can appear when secrets are stored via the
        Databricks CLI on Windows (e.g. '{"user":"x"}' stored with literal
        single quotes).

        Returns
        -------
        (username, password) : both as plain strings.

        Raises
        ------
        ValueError  if the secret is missing, not valid JSON, or lacks the
                    required 'password' field.
        """
        raw = self.get(scope, key)

        # Strip surrounding whitespace, then strip one layer of wrapping
        # single or double quotes (common Windows CLI artefact).
        cleaned = raw.strip()
        if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in ("'", '"'):
            cleaned = cleaned[1:-1].strip()

        try:
            payload = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Secret scope='{scope}' key='{key}' must be a valid JSON object "
                f"(e.g. {{\"username\": \"x\", \"password\": \"y\"}}). "
                f"Received (first 120 chars): {repr(raw[:120])}. "
                f"JSON parse error: {exc}"
            ) from exc

        # Accept both 'username' and 'user' as the username key
        username = payload.get("username") or payload.get("user") or ""
        password = payload.get("password")

        if password is None:
            raise ValueError(
                f"Secret scope='{scope}' key='{key}' JSON must contain a "
                f"'password' field. Keys found: {list(payload.keys())}"
            )
        return username, password

    # ── High-level: parse an arbitrary JSON credential blob ───────────────────

    def get_json(self, scope: str, key: str) -> dict:
        """
        Retrieve and parse a JSON secret with an arbitrary shape (unlike
        get_credentials, not limited to username/password) — e.g. the
        {"tenant_id", "client_id", "client_secret"} blob used by
        GraphMailNotifier for its Azure AD app registration.
        """
        raw = self.get(scope, key)

        cleaned = raw.strip()
        if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in ("'", '"'):
            cleaned = cleaned[1:-1].strip()

        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Secret scope='{scope}' key='{key}' must be a valid JSON object. "
                f"Received (first 120 chars): {repr(raw[:120])}. "
                f"JSON parse error: {exc}"
            ) from exc
