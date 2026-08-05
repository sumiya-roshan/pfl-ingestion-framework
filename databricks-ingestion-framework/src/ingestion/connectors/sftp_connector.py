"""
SFTP connector — downloads matching files to a staging area via paramiko,
then loads them into a Spark DataFrame based on the configured file_format.

Field mapping from new config tables
-------------------------------------
  config_source_system
    .sftp_root_path           → root directory on the SFTP server
    .sftp_file_pattern        → default glob (e.g. '*.csv'); overridable per object
    .sftp_host_key_fingerprint→ optional host-key check (logged; strict enforcement
                                can be added when paramiko host-key policy is enabled)
    .landing_volume_path      → local staging path (Databricks Volume recommended
                                in production, e.g. /Volumes/main/landing/sftp)
    .secret_scope             → Databricks secret scope
    .secret_key_credentials   → key holding JSON {"username":..,"password":..}

  ingestion_config
    .source_schema            → optional sub-folder under sftp_root_path
    .source_object_name       → specific file name or additional glob token
    .source_filter            → (unused by SFTP; reserved for future filtering)
    .file_format              → csv | json | parquet | fixed_width
    .sheet_name               → used for Excel-like formats (future extension)

Remote path resolution
-----------------------
  remote_dir = sftp_root_path  /  source_schema (if set)
  pattern    = sftp_file_pattern from source system
               (if source_object_name looks like a glob, it is used instead)

File archival
-------------
Processed files are moved to  <remote_dir>/archive/  after download.
This is enabled by default; to disable, set source_filter = 'no_archive'
(a convention until a dedicated boolean column is added to ingestion_config).
"""
import fnmatch
import os
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame

from .base_connector import BaseConnector

# Default staging root when landing_volume_path is not set in config_source_system.
# In production, override this via landing_volume_path pointing to a Databricks Volume.
_FALLBACK_STAGING_ROOT = "/tmp/sftp_staging"

# Convention: set source_filter = 'no_archive' to skip post-download archival.
_NO_ARCHIVE_FLAG = "no_archive"


class SftpConnector(BaseConnector):

    # ── Private helpers ───────────────────────────────────────────────────────

    def _get_sftp_client(self):
        """Open a paramiko SFTP client and return (sftp, transport)."""
        import paramiko

        ss = self.source_system
        username, password = self.secrets.get_credentials(
            ss.secret_scope, ss.secret_key_credentials
        )

        transport = paramiko.Transport((ss.host, ss.port or 22))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return sftp, transport

    def _resolve_remote_dir(self) -> str:
        """
        Build the full remote directory path.
        remote_dir = sftp_root_path / source_schema   (source_schema is optional sub-folder)
        """
        ss = self.source_system
        io = self.ingest_obj
        root = (ss.sftp_root_path or "/").rstrip("/")
        if io.source_schema:
            return f"{root}/{io.source_schema.strip('/')}"
        return root

    def _resolve_file_pattern(self) -> str:
        """
        Determine the glob pattern to match remote files.
        If source_object_name contains glob characters it is used directly;
        otherwise sftp_file_pattern from the source system is used.
        """
        io = self.ingest_obj
        ss = self.source_system
        obj_name = io.source_object_name or ""
        glob_chars = {"*", "?", "[", "]"}
        if any(c in obj_name for c in glob_chars):
            return obj_name
        return ss.sftp_file_pattern or "*"

    def _resolve_staging_dir(self) -> str:
        """Return the local staging directory for downloaded files."""
        ss = self.source_system
        io = self.ingest_obj
        root = (ss.landing_volume_path or _FALLBACK_STAGING_ROOT).rstrip("/")
        return os.path.join(root, str(io.ingestion_object_id))

    def _list_matching_files(self, sftp, remote_dir: str, pattern: str) -> List[str]:
        all_files = sftp.listdir(remote_dir)
        return [f for f in all_files if fnmatch.fnmatch(f, pattern)]

    def _download_files(
        self, sftp, remote_dir: str, filenames: List[str], local_dir: str
    ) -> List[str]:
        os.makedirs(local_dir, exist_ok=True)
        local_paths = []
        for filename in filenames:
            remote_path = f"{remote_dir.rstrip('/')}/{filename}"
            local_path = os.path.join(local_dir, filename)
            sftp.get(remote_path, local_path)
            local_paths.append(local_path)
        return local_paths

    def _load_files(self, local_paths: List[str]) -> DataFrame:
        """Load downloaded files into a Spark DataFrame based on file_format."""
        fmt = (self.ingest_obj.file_format or "csv").lower()
        reader = self.spark.read

        if fmt == "csv":
            return (
                reader
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(local_paths)
            )
        if fmt == "json":
            return reader.json(local_paths)
        if fmt == "parquet":
            return reader.parquet(local_paths)
        if fmt in ("fixed_width", "fixedwidth"):
            # Fixed-width files require a schema; use inferSchema as a best-effort default.
            return (
                reader
                .option("header", "false")
                .option("inferSchema", "true")
                .csv(local_paths)
            )
        raise ValueError(
            f"Unsupported file_format='{fmt}' for SFTP connector. "
            f"Supported: csv, json, parquet, fixed_width."
        )

    def _archive_files(self, sftp, remote_dir: str, filenames: List[str]) -> None:
        """Move processed files into <remote_dir>/archive/ on the SFTP server."""
        archive_dir = f"{remote_dir.rstrip('/')}/archive"
        try:
            sftp.stat(archive_dir)
        except FileNotFoundError:
            sftp.mkdir(archive_dir)
        for filename in filenames:
            sftp.rename(
                f"{remote_dir.rstrip('/')}/{filename}",
                f"{archive_dir}/{filename}",
            )

    # ── Public extract ────────────────────────────────────────────────────────

    def extract(self, watermark_start: Optional[str]) -> Tuple[DataFrame, Optional[str]]:
        sftp, transport = self._get_sftp_client()
        remote_dir  = self._resolve_remote_dir()
        pattern     = self._resolve_file_pattern()
        local_dir   = self._resolve_staging_dir()

        # archive unless source_filter flags 'no_archive'
        archive = (self.ingest_obj.source_filter or "").strip().lower() != _NO_ARCHIVE_FLAG

        try:
            matched = self._list_matching_files(sftp, remote_dir, pattern)
            if not matched:
                raise FileNotFoundError(
                    f"No files matched pattern='{pattern}' in remote_dir='{remote_dir}'"
                )

            local_paths = self._download_files(sftp, remote_dir, matched, local_dir)
            df = self._load_files(local_paths)

            if archive:
                self._archive_files(sftp, remote_dir, matched)

            # SFTP ingestion is file-batch oriented; watermark is not applicable.
            return df, None
        finally:
            sftp.close()
            transport.close()
