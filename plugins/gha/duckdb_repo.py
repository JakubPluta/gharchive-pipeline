import contextlib
from typing import Optional, Union

import duckdb
import dataclasses
import dotenv
import os
import logging


logger = logging.getLogger(__name__)


class DuckDBConnectionError(Exception):
    """Base class for DuckDB connection errors."""


@dataclasses.dataclass
class DuckDBConfiguration:
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_endpoint: str
    s3_use_ssl: bool = False
    s3_url_style: str = "path"

    @classmethod
    def from_env(cls) -> "DuckDBConfiguration":
        """
        Load configuration from environment variables and .env file.

        The following environment variables are expected to be set:

        - MINIO_ROOT_USER: The AWS access key ID.
        - MINIO_ROOT_PASSWORD: The AWS secret access key.
        - MINIO_ENDPOINT: The S3 endpoint URL. Defaults to "localhost:9000".

        If a .env file is present, it will be loaded and the environment variables
        will be overwritten with the values from the .env file.

        Returns:
            DuckDBConfiguration: The loaded configuration.

        Raises:
            KeyError: If MINIO_ROOT_USER or MINIO_ROOT_PASSWORD is not set.
        """
        dotenv.load_dotenv()
        try:
            aws_access_key_id = os.environ["MINIO_ROOT_USER"]
            aws_secret_access_key = os.environ["MINIO_ROOT_PASSWORD"]
            s3_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        except KeyError:
            raise Exception("MINIO_ROOT_USER and MINIO_ROOT_PASSWORD must be set in .env file or environment variables")

        return cls(aws_access_key_id, aws_secret_access_key, s3_endpoint)

    @property
    def as_dict(self) -> dict:
        """
        Convert configuration to a dictionary.

        Booleans are converted to strings with value "true" or "false".

        Returns:
            dict: The configuration as a dictionary.
        """
        return {k: v if not isinstance(v, bool) else str(v).lower() for k, v in dataclasses.asdict(self)}


class DuckdbRepository:
    """DuckDB repository class for executing SQL queries and managing transactions."""

    def __init__(self, duckdb_config: DuckDBConfiguration, db_path: Optional[str] = None):
        self._config: DuckDBConfiguration = duckdb_config
        self.db_path: str = db_path or ":memory:"
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

        self._in_transaction: bool = False

    def connect(self) -> None:
        """Connect to the DuckDB database."""
        logger.info("connecting to duckdb")
        try:
            self.conn: duckdb.DuckDBPyConnection = duckdb.connect()
            self._configure_s3_connection()
        except Exception as e:
            logger.error(f"Error connecting to duckdb: {e}")
            raise DuckDBConnectionError(f"Error connecting to duckdb: {e}")

    def close(self):
        """Close the DuckDB connection.

        If the connection is not open, this is a no-op.
        """
        if self.conn is not None:
            logger.info("closing duckdb connection")
            self.conn.close()
            self.conn = None

    def __enter__(self) -> "DuckdbRepository":
        """Enter the context manager and connect to the DuckDB database."""
        if self.conn is not None:
            logger.warning("duckdb connection already open")
            return self
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager and close the DuckDB connection."""
        self.close()
        if exc_type is not None:
            raise exc_val

    def _configure_s3_connection(self) -> None:
        """
        Configure an existing DuckDB connection to use the S3 connection settings in the given DuckDBConfiguration.
        The httpfs extension is installed and loaded if not already done.
        """
        self.conn.install_extension("httpfs")
        self.conn.load_extension("httpfs")
        for k, v in self._config.as_dict.items():
            self.conn.execute(f"SET {k}='{v}'")

    def execute(self, query: str, params: Optional[Union[dict, list, tuple]] = None) -> duckdb.DuckDBPyConnection:
        """
        Execute a SQL query on the DuckDB connection.

        Args:
            query (str): The SQL query to execute.
            params (Optional[Union[dict, list, tuple]], optional): Parameters to pass to the query. Defaults to None.

        Returns:
            duckdb.DuckDBPyConnection: The result of the query execution.

        Raises:
            DuckDBConnectionError: If the DuckDB connection is not open.
            Exception: If an error occurs during query execution.
        """

        if self.conn is None:
            raise DuckDBConnectionError("duckdb connection is not open")
        try:
            return self.conn.execute(query, params)
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    @contextlib.contextmanager
    def transaction(self) -> None:
        """Transaction context manager for the DuckDB connection.

        Example:
            >>> duckdb_repo = DuckdbRepository()
            >>> with duckdb_repo.transaction():
            ...     duckdb_repo.execute("INSERT INTO mytable VALUES (1, 2)")

        Raises:
            DuckDBConnectionError: If no active connection is available.
            DuckDBConnectionError: If already in a transaction.
        """
        if self.conn is None:
            raise DuckDBConnectionError("No active connection")

        if self._in_transaction:
            raise DuckDBConnectionError("Already in transaction")

        try:
            self._in_transaction = True
            self.conn.begin()
            yield
            self.conn.commit()
        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            self.conn.rollback()
            raise
        finally:
            self._in_transaction = False

    def execute_transaction(self, query: str, params: Optional[Union[dict, list, tuple]] = None) -> None:
        """
        Execute a SQL query within a transaction.

        This is a convenience wrapper around `execute` and `transaction`. It
        executes the query within a transaction context, so if an error occurs,
        the changes will be rolled back.

        Args:
            query: The SQL query to execute.
            params: Optional parameters to pass to the query.

        Raises:
            DuckDBConnectionError: If no active connection is available.
            DuckDBConnectionError: If already in a transaction.
            Exception: If the query execution fails.
        """
        with self.transaction():
            self.execute(query, params)
