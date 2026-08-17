"""
SilverProcessor — triggers the Silver transformation notebook for a specific
table using dbutils.notebook.run().

This runs within the same Databricks Job as the monitor task (Task 2),
on Task 2's own cluster. No separate Silver Job is needed.

dbutils is passed in from the monitor notebook since it is a
notebook-level object and cannot be imported as a module.
"""
import logging

log = logging.getLogger("ingestion_framework")


class SilverProcessor:
    """
    Runs the Silver transformation notebook for one table via
    dbutils.notebook.run().

    Parameters
    ----------
    dbutils              : Databricks dbutils object (passed from monitor notebook)
    silver_notebook_path : Workspace path to the Silver transformation notebook
                           e.g. /Workspace/Shared/pfl-ingestion-framework/src/silver/silver_transform
    timeout_seconds      : Max seconds to wait for the Silver notebook to finish (default 3600)
    """

    def __init__(self, dbutils, silver_notebook_path: str, timeout_seconds: int = 3600):
        self.dbutils               = dbutils
        self.silver_notebook_path  = silver_notebook_path
        self.timeout_seconds       = timeout_seconds

    def trigger(
        self,
        config_id: int,
        table_name: str,
        config_master_id: int,
        source_system_id: int,
        landing_volume_path: str,
    ) -> dict:
        """
        Runs the Silver notebook synchronously for one table and returns a result dict.

        The Silver notebook is expected to accept these widget parameters:
          - config_id           : int    (identifies the ingestion config row)
          - table_name          : str    (the Bronze target table to process)
          - config_master_id    : int
          - source_system_id    : int
          - landing_volume_path : str
          
        Returns dict with keys: config_id, name, status, error
        """
        log.info(
            f"[SILVER] Running Silver notebook for config_id={config_id} table='{table_name}'"
        )

        exit_value = self.dbutils.notebook.run(
            self.silver_notebook_path,
            self.timeout_seconds,
            {
                "config_id":           str(config_id),
                "table_name":          table_name,
                "config_master_id":    str(config_master_id),
                "source_system_id":    str(source_system_id),
                "landing_volume_path": landing_volume_path or "",
            },
        )

        log.info(
            f"[SILVER] Silver notebook finished for table='{table_name}' "
            f"exit_value='{exit_value}'"
        )
        return {
            "config_id": config_id,
            "table_name": table_name,
            "status":    "SUCCESS",
            "exit_value": exit_value,
            "error":     None,
        }


# """
# SilverProcessor — triggers the Silver transformation notebook for a specific
# table using dbutils.notebook.run().

# This runs within the same Databricks Job as the monitor task (Task 2),
# on Task 2's own cluster. No separate Silver Job is needed.

# dbutils is passed in from the monitor notebook since it is a
# notebook-level object and cannot be imported as a module.
# """
# import logging

# log = logging.getLogger("ingestion_framework")


# class SilverProcessor:
#     """
#     Runs the Silver transformation notebook for one table via
#     dbutils.notebook.run().

#     Parameters
#     ----------
#     dbutils              : Databricks dbutils object (passed from monitor notebook)
#     silver_notebook_path : Workspace path to the Silver transformation notebook
#                            e.g. /Workspace/Shared/pfl-ingestion-framework/src/silver/silver_transform
#     timeout_seconds      : Max seconds to wait for the Silver notebook to finish (default 3600)
#     """

#     def __init__(self, dbutils, silver_notebook_path: str, timeout_seconds: int = 3600):
#         self.dbutils               = dbutils
#         self.silver_notebook_path  = silver_notebook_path
#         self.timeout_seconds       = timeout_seconds

#     def trigger(self, config_id: int, table_name: str) -> dict:
#         """
#         Runs the Silver notebook synchronously for one table and returns a result dict.

#         The Silver notebook is expected to accept these widget parameters:
#           - config_id  : int    (identifies the ingestion config row)
#           - table_name : str    (the Bronze target table to process)

#         Returns dict with keys: config_id, name, status, error
#         """
#         log.info(
#             f"[SILVER] Running Silver notebook for config_id={config_id} table='{table_name}'"
#         )

#         exit_value = self.dbutils.notebook.run(
#             self.silver_notebook_path,
#             self.timeout_seconds,
#             {
#                 "config_id":  str(config_id),
#                 "table_name": table_name,
#             },
#         )

#         log.info(
#             f"[SILVER] Silver notebook finished for table='{table_name}' "
#             f"exit_value='{exit_value}'"
#         )
#         return {
#             "config_id": config_id,
#             "table_name": table_name,
#             "status":    "SUCCESS",
#             "exit_value": exit_value,
#             "error":     None,
#         }