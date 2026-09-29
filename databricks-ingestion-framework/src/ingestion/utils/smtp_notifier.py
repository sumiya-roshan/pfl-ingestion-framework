"""
Sends notification emails via plain SMTP (Outlook/Office 365,
smtp.office365.com:587, STARTTLS) using a corporate mailbox's own
email address + password — replaces the Logic App. No Graph API, no
OAuth app registration, no AWS: just smtplib.

send_email() matches GraphMailNotifier's/SesMailNotifier's exact
signature — a drop-in replacement in orchestrator.py's __init__ (just
swap which class gets constructed). Never raises: any failure is
logged and swallowed, same contract as the other notifiers, since a
notification failure must never fail the table's actual
ingestion/Silver run.

Credentials: TEMPORARY — hardcoded mailbox email/password below for local
testing (TODO: move to a Databricks secret scope via
SecretResolver.get_credentials(), same convention as
config_source_system.secret_key_credentials, before this is committed
anywhere real credentials shouldn't live in git history). For Office 365
this must be an app password if the tenant has MFA/modern-auth-only
enforced on SMTP AUTH.
"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

SMTP_HOST = "smtp.office365.com"
SMTP_PORT = 587

# TEMPORARY — hardcoded mailbox credentials for testing. Fill in locally;
# do NOT commit real values here. Move to a Databricks secret scope
# (SecretResolver.get_credentials, same pattern GraphMailNotifier's secret
# lookup uses) before this goes anywhere shared.
SENDER_EMAIL = "benny.charles@ganitinc.com"
SENDER_PASSWORD = ""


class SmtpMailNotifier:
    def __init__(
        self,
        dbutils=None,
        logger=None,
        sender_email: str = SENDER_EMAIL,
        sender_password: str = SENDER_PASSWORD,
        smtp_host: str = SMTP_HOST,
        smtp_port: int = SMTP_PORT,
    ):
        self.logger = logger
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port

    # ── 1. Send — the actual notification ────────────────────────────────

    def _send(self, recipients: list[str], subject: str, body: str) -> None:
        message = MIMEMultipart()
        message["From"] = self.sender_email
        message["To"] = ", ".join(recipients)
        message["Subject"] = subject
        message.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as server:
            server.starttls()
            server.login(self.sender_email, self.sender_password)
            server.sendmail(self.sender_email, recipients, message.as_string())

    # ── 2. Public entry point ─────────────────────────────────────────────

    def send_email(
        self,
        subject: str,
        body: str,
        recipients: list[str] | None,
        config_id: int | None = None,
    ) -> None:
        """
        Single entry point for both success and failure mail — same contract
        as GraphMailNotifier.send_email(). Never raises.
        """
        if not recipients:
            self._log_warning(
                f"No recipients configured for config_id={config_id} — skipping email."
            )
            return

        try:
            self._send(recipients, subject, body)
        except Exception as exc:
            self._log_warning(f"Failed to send email for config_id={config_id}: {exc}")

    def _log_warning(self, msg: str) -> None:
        if self.logger:
            self.logger.warning(f"[NOTIFIER] {msg}")
        else:
            print(f"[NOTIFIER] WARNING: {msg}")

 