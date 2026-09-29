"""
Sends notification emails via AWS SES, replacing GraphMailNotifier.

send_email() matches GraphMailNotifier's exact signature — a drop-in
replacement in orchestrator.py's __init__ (just swap which class gets
constructed). Never raises: any failure is logged and swallowed, same
contract as GraphMailNotifier, since a notification failure must never
fail the table's actual ingestion/Silver run.

Credentials: no IAM instance profile on this cluster, so the AWS access
key/secret are hardcoded constants below for now (TEMPORARY — move these to
a Databricks secret scope via SecretResolver.get_json(), the same way
GraphMailNotifier handles its tenant_id/client_id/client_secret, before
this is committed anywhere real keys shouldn't live in git history).

Recipient verification: SES sandbox accounts can only send to verified
addresses, and a single unverified recipient in one send_email() call fails
the WHOLE call (all-or-nothing per API call) — not per-recipient. So every
call first checks the recipient list against SES's identity verification
status and (re-)triggers a verification email for anyone not yet verified
(harmless/no-op once the account has SES production access, since
verification stops being required at all). It then attempts to send to the
full list; only if that whole call is rejected (the sandbox "one unverified
recipient blocks everyone" case) does it fall back to resending to just the
already-verified subset, so verified recipients aren't collateral damage
while others are still pending verification.
"""

import boto3
from botocore.exceptions import ClientError

# Must already be a verified SES identity (or its domain must be) — this is
# a one-time infrastructure-side setup, not something checked per-run.
SENDER_EMAIL = "benny.charles@ganitinc.com"  # TODO: set to your verified sender

DEFAULT_REGION = "ap-south-1"

# TEMPORARY — hardcoded AWS credentials. Fill in locally; do NOT commit real
# values here. Move to a Databricks secret scope (SecretResolver.get_json,
# same pattern GraphMailNotifier uses) before this goes anywhere shared.
AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""


class SesMailNotifier:
    def __init__(
        self,
        dbutils=None,
        logger=None,
        sender_email: str = SENDER_EMAIL,
        region_name: str = DEFAULT_REGION,
        aws_access_key_id: str = AWS_ACCESS_KEY_ID,
        aws_secret_access_key: str = AWS_SECRET_ACCESS_KEY,
    ):
        self.logger = logger
        self.sender_email = sender_email
        self.client = boto3.client(
            "ses",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    # ── 1. Verification check/trigger — runs before every send ──────────────

    def _ensure_verified(self, recipients: list[str]) -> set[str]:
        """
        Checks each recipient's SES verification status; (re-)triggers a
        verification email for anyone not already Success-verified. Returns
        the subset that IS currently verified (used only as a fallback if
        the full send below gets rejected). Best-effort — a failure here
        must not block the actual send attempt.
        """
        verified: set[str] = set()
        try:
            attrs = self.client.get_identity_verification_attributes(
                Identities=recipients
            )["VerificationAttributes"]
        except Exception as exc:
            self._log_warning(f"Could not check SES verification status: {exc}")
            attrs = {}

        for addr in recipients:
            status = attrs.get(addr, {}).get("VerificationStatus")
            if status == "Success":
                verified.add(addr)
                continue
            # Not verified (or unknown/pending) — (re-)trigger AWS's
            # verification email. Safe to call repeatedly; AWS just resends
            # the same verification link.
            try:
                self.client.verify_email_identity(EmailAddress=addr)
                self._log_warning(
                    f"Recipient {addr} is not SES-verified — verification email (re-)sent."
                )
            except Exception as exc:
                self._log_warning(f"Could not send SES verification email to {addr}: {exc}")

        return verified

    # ── 2. Send — the actual notification ────────────────────────────────

    def _send(self, recipients: list[str], subject: str, body: str) -> None:
        self.client.send_email(
            Source=self.sender_email,
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {"Text": {"Data": body, "Charset": "UTF-8"}},
            },
        )

    # ── 3. Public entry point — ties steps 1 and 2 together ──────────────────

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

        verified = self._ensure_verified(recipients)

        try:
            self._send(recipients, subject, body)
            return
        except ClientError as exc:
            # Sandbox mode: one unverified recipient fails the whole call.
            # Fall back to just the verified subset so they still get it.
            self._log_warning(
                f"Send to full recipient list failed for config_id={config_id} "
                f"({exc}); retrying with verified recipients only."
            )
        except Exception as exc:
            self._log_warning(f"Failed to send email for config_id={config_id}: {exc}")
            return

        if not verified:
            self._log_warning(
                f"No verified recipients available for config_id={config_id} — email not sent."
            )
            return
        try:
            self._send(list(verified), subject, body)
        except Exception as exc:
            self._log_warning(
                f"Fallback send to verified recipients also failed for config_id={config_id}: {exc}"
            )

    def _log_warning(self, msg: str) -> None:
        if self.logger:
            self.logger.warning(f"[NOTIFIER] {msg}")
        else:
            print(f"[NOTIFIER] WARNING: {msg}")
