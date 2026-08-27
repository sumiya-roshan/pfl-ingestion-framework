"""
Sends failure-notification emails via Microsoft Graph API
(POST /users/{mailbox}/sendMail), authenticated as an Azure AD app
registration using the client-credentials (app-only) flow.

Credentials, sender mailbox, and recipient list come from
notification_config.py — hardcoded for now (see that file's docstring).

Best-effort by design: every public method catches its own exceptions and
logs a warning instead of raising — a notification failure must never fail
the table's actual ingestion/Silver run. Callers may still wrap calls in
their own try/except as belt-and-braces (see orchestrator.py).
"""
import threading
import time
from typing import List, Optional

import requests

from . import notification_config as cfg

_TOKEN_URL_TEMPLATE     = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
_SEND_MAIL_URL_TEMPLATE = "https://graph.microsoft.com/v1.0/users/{mailbox}/sendMail"
_GRAPH_SCOPE            = "https://graph.microsoft.com/.default"


class GraphMailNotifier:

    def __init__(self, logger=None):
        self.logger = logger
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._token_lock = threading.Lock()

    # ── Public API ────────────────────────────────────────────────────────────

    def send_failure_email(
        self,
        stage: str,
        source_name: str,
        table_name: str,
        config_id: int,
        run_id: Optional[str],
        error_message: str,
        recipients: Optional[List[str]] = None,
    ) -> None:
        """Never raises — logs and swallows any failure to send."""
        try:
            to = recipients or cfg.FAILURE_NOTIFICATION_RECIPIENTS
            if not to:
                self._log_warning("No failure-notification recipients configured — skipping email.")
                return

            subject = f"[Pipeline FAILURE] {stage} — {source_name}.{table_name} (config_id={config_id})"
            body = (
                f"Stage failed: {stage}\n"
                f"Source: {source_name}\n"
                f"Table: {table_name}\n"
                f"Config ID: {config_id}\n"
                f"Run ID: {run_id}\n\n"
                f"Error:\n{error_message}"
            )
            self._send_mail(to_recipients=to, subject=subject, body=body)
        except Exception as exc:
            self._log_warning(f"Failed to send failure-notification email for stage={stage}: {exc}")

    # ── Internal: auth + send ────────────────────────────────────────────────

    def _get_access_token(self) -> str:
        with self._token_lock:
            if self._token and time.time() < self._token_expires_at - 60:
                return self._token

            resp = requests.post(
                _TOKEN_URL_TEMPLATE.format(tenant_id=cfg.GRAPH_TENANT_ID),
                data={
                    "grant_type":    "client_credentials",
                    "client_id":     cfg.GRAPH_CLIENT_ID,
                    "client_secret": cfg.GRAPH_CLIENT_SECRET,
                    "scope":         _GRAPH_SCOPE,
                },
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
            self._token = payload["access_token"]
            self._token_expires_at = time.time() + int(payload.get("expires_in", 3600))
            return self._token

    def _send_mail(self, to_recipients: List[str], subject: str, body: str) -> None:
        token = self._get_access_token()
        url = _SEND_MAIL_URL_TEMPLATE.format(mailbox=cfg.SENDER_MAILBOX)
        message = {
            "message": {
                "subject": subject,
                "body": {"contentType": "Text", "content": body},
                "toRecipients": [{"emailAddress": {"address": addr}} for addr in to_recipients],
            },
            "saveToSentItems": "false",
        }
        resp = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=message,
            timeout=30,
        )
        resp.raise_for_status()

    def _log_warning(self, msg: str) -> None:
        if self.logger:
            self.logger.warning(f"[NOTIFIER] {msg}")
        else:
            print(f"[NOTIFIER] WARNING: {msg}")
