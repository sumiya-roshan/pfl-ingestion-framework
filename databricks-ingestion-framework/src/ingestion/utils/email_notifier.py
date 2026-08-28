"""
Sends notification emails via Microsoft Graph API (POST /users/{mailbox}/sendMail),
authenticated as an Azure AD app registration using the client-credentials
(app-only) flow.

Credentials (tenant_id, client_id, client_secret) come from a Databricks
secret scope — same convention as config_source_system.secret_key_credentials
(see secrets.py: SecretResolver.get_json). The secret value must be a JSON
object: {"tenant_id": "...", "client_id": "...", "client_secret": "..."}.
Sender mailbox is NOT secret and is a plain constant below (safe to commit).

send_email() is the single entry point for both success and failure mail —
the subject/body text is built entirely by the caller (see orchestrator.py);
this module only handles delivery. It never raises: token acquisition and
the send itself each retry once independently, and any failure surviving
both retries is logged and swallowed, never propagated — a notification
failure must never fail the table's actual ingestion/Silver run, so callers
do not need their own try/except around it.
"""
import threading
import time
from typing import List, Optional

import requests

from .secrets import SecretResolver

_TOKEN_URL_TEMPLATE     = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
_SEND_MAIL_URL_TEMPLATE = "https://graph.microsoft.com/v1.0/users/{mailbox}/sendMail"
_GRAPH_SCOPE            = "https://graph.microsoft.com/.default"
_MAX_ATTEMPTS           = 2   
_RETRY_DELAY_SEC        = 2

# Where the Graph API app-registration credentials live — a single secret
# holding {"tenant_id": "...", "client_id": "...", "client_secret": "..."}.
GRAPH_SECRET_SCOPE = "graph-secrets-scope"
GRAPH_SECRET_KEY   = "graph-secrets"

# Mailbox the app sends as — needs Mail.Send app permission for this mailbox.
SENDER_MAILBOX = "siriki.prasanthi@ganitinc.com"


class GraphMailNotifier:

    def __init__(
        self,
        dbutils=None,
        logger=None,
        secret_scope: str = GRAPH_SECRET_SCOPE,
        secret_key: str = GRAPH_SECRET_KEY,
    ):
        self.logger       = logger
        self.secrets      = SecretResolver(dbutils)
        self.secret_scope = secret_scope
        self.secret_key   = secret_key
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._token_lock = threading.Lock()

    # ── 1. Auth — lowest-level step, called first in the flow ────────────────

    def _get_access_token(self) -> str:
        """Retries once on failure. Raises if both attempts fail — caught by
        send_email(), the only public entry point."""
        with self._token_lock:
            if self._token and time.time() < self._token_expires_at - 60:
                return self._token

            creds = self.secrets.get_json(self.secret_scope, self.secret_key)
            last_exc: Optional[Exception] = None
            for attempt in range(_MAX_ATTEMPTS):
                try:
                    resp = requests.post(
                        _TOKEN_URL_TEMPLATE.format(tenant_id=creds["tenant_id"]),
                        data={
                            "grant_type":    "client_credentials",
                            "client_id":     creds["client_id"],
                            "client_secret": creds["client_secret"],
                            "scope":         _GRAPH_SCOPE,
                        },
                        timeout=30,
                    )
                    resp.raise_for_status()
                    payload = resp.json()
                    self._token = payload["access_token"]
                    self._token_expires_at = time.time() + int(payload.get("expires_in", 3600))
                    return self._token
                except Exception as exc:
                    last_exc = exc
                    if attempt < _MAX_ATTEMPTS - 1:
                        time.sleep(_RETRY_DELAY_SEC)
            raise last_exc

    # ── 2. Send — next step in the flow, needs a token from step 1 ───────────

    def _post_send_mail(self, token: str, to_recipients: List[str], subject: str, body: str) -> None:
        """Retries once on failure. Raises if both attempts fail — caught by
        send_email(), the only public entry point."""
        url = _SEND_MAIL_URL_TEMPLATE.format(mailbox=SENDER_MAILBOX)
        message = {
            "message": {
                "subject": subject,
                "body": {"contentType": "Text", "content": body},
                "toRecipients": [{"emailAddress": {"address": addr}} for addr in to_recipients],
            },
            "saveToSentItems": "false",
        }
        last_exc: Optional[Exception] = None
        for attempt in range(_MAX_ATTEMPTS):
            try:
                resp = requests.post(
                    url,
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json=message,
                    timeout=30,
                )
                resp.raise_for_status()
                return
            except Exception as exc:
                last_exc = exc
                if attempt < _MAX_ATTEMPTS - 1:
                    time.sleep(_RETRY_DELAY_SEC)
        raise last_exc

    # ── 3. Public entry point — ties steps 1 and 2 together ──────────────────

    def send_email(
        self,
        subject: str,
        body: str,
        recipients: Optional[List[str]],
        config_id: Optional[int] = None,
    ) -> None:
        """
        Single entry point for both success and failure mail — build the
        subject/body in the caller (orchestrator.py knows what happened;
        this module doesn't). recipients comes from that table's own
        config-table 'recipients' column (IngestionTaskConfig.recipient_list)
        — there is no fallback list, so a table with no recipients
        configured is skipped rather than notifying anyone. Never raises.
        """
        if not recipients:
            self._log_warning(f"No recipients configured for config_id={config_id} — skipping email.")
            return
        try:
            token = self._get_access_token()
            self._post_send_mail(token, recipients, subject, body)
        except Exception as exc:
            self._log_warning(f"Failed to send email for config_id={config_id}: {exc}")

    def _log_warning(self, msg: str) -> None:
        if self.logger:
            self.logger.warning(f"[NOTIFIER] {msg}")
        else:
            print(f"[NOTIFIER] WARNING: {msg}")
