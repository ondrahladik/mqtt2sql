import logging
import threading
import time
from collections.abc import Mapping

import mysql.connector
from mysql.connector import Error as MysqlConnectorError
from mysql.connector.abstracts import MySQLConnectionAbstract

from core.exceptions import DatabaseError
from core.models import AppConfig, ConnectorConfig, MysqlConfig
from core.ports import DatabaseGateway


class MysqlDatabaseGateway(DatabaseGateway):
    def __init__(self, config: MysqlConfig, app_config: AppConfig, logger: logging.Logger) -> None:
        self._config = config
        self._app_config = app_config
        self._logger = logger
        self._connection: MySQLConnectionAbstract | None = None
        self._lock = threading.Lock()
        self._table_columns_cache: dict[str, set[str]] = {}

    def insert(self, connector: ConnectorConfig, payload: Mapping[str, object]) -> None:
        table_name = self._sanitize_identifier(connector.table, "table")
        values = self._prepare_values(payload)
        if not values:
            self._logger.warning("Skipping insert for connector '%s' because payload is empty", connector.name)
            return
        with self._lock:
            columns = self._get_table_columns(table_name)
            if "created_at" in columns and "created_at" not in values:
                values["created_at"] = None
            filtered_values = {column: value for column, value in values.items() if column in columns}
            if not filtered_values:
                self._logger.warning("Skipping insert for connector '%s' because no payload keys match table '%s'", connector.name, table_name)
                return
            statement, parameters = self._build_insert_statement(table_name, filtered_values)
            self._execute(statement, parameters)
            self._logger.debug("Inserted message for connector '%s' into table '%s'", connector.name, table_name)

    def close(self) -> None:
        with self._lock:
            if self._connection is not None and self._connection.is_connected():
                self._connection.close()
            self._connection = None

    def _prepare_values(self, payload: Mapping[str, object]) -> dict[str, object]:
        prepared: dict[str, object] = {}
        for key, value in payload.items():
            prepared[self._sanitize_identifier(str(key), "column")] = value
        return prepared

    def _get_connection(self) -> MySQLConnectionAbstract:
        if self._connection is None or not self._connection.is_connected():
            self._connect_with_retry()
        assert self._connection is not None
        try:
            self._connection.ping(reconnect=True, attempts=1, delay=0)
        except MysqlConnectorError:
            self._logger.warning("Database ping failed, reconnecting")
            self._connect_with_retry()
        assert self._connection is not None
        return self._connection

    def _connect_with_retry(self) -> None:
        while True:
            try:
                self._connection = mysql.connector.connect(
                    host=self._config.host,
                    port=self._config.port,
                    user=self._config.user,
                    password=self._config.password,
                    database=self._config.database,
                    autocommit=False,
                )
                self._table_columns_cache.clear()
                self._logger.info("Connected to MySQL/MariaDB")
                return
            except MysqlConnectorError as error:
                self._logger.error("Database connection failed: %s", error)
                time.sleep(self._app_config.reconnect_interval)

    def _get_table_columns(self, table_name: str) -> set[str]:
        cached_columns = self._table_columns_cache.get(table_name)
        if cached_columns is not None:
            return cached_columns
        connection = self._get_connection()
        query = (
            "SELECT COLUMN_NAME FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s"
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, (self._config.database, table_name))
                columns = {row[0] for row in cursor.fetchall()}
        except MysqlConnectorError as error:
            raise DatabaseError(f"Unable to load schema for table '{table_name}': {error}") from error
        if not columns:
            raise DatabaseError(f"Table '{table_name}' does not exist or has no columns")
        self._table_columns_cache[table_name] = columns
        return columns

    def _build_insert_statement(self, table_name: str, values: Mapping[str, object]) -> tuple[str, tuple[object, ...]]:
        columns = list(values.keys())
        placeholders: list[str] = []
        parameters: list[object] = []
        for column in columns:
            if column == "created_at" and values[column] is None:
                placeholders.append("CURRENT_TIMESTAMP")
            else:
                placeholders.append("%s")
                parameters.append(values[column])
        column_sql = ", ".join(self._quote_identifier(column) for column in columns)
        values_sql = ", ".join(placeholders)
        statement = f"INSERT INTO {self._quote_identifier(table_name)} ({column_sql}) VALUES ({values_sql})"
        return statement, tuple(parameters)

    def _execute(self, statement: str, parameters: tuple[object, ...]) -> None:
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(statement, parameters)
            connection.commit()
        except MysqlConnectorError as error:
            connection.rollback()
            self._logger.error("Database insert failed: %s", error)
            self._connection = None
            raise DatabaseError(f"Unable to insert row: {error}") from error

    def _sanitize_identifier(self, value: str, identifier_type: str) -> str:
        if not value or not value.strip():
            raise DatabaseError(f"Invalid {identifier_type} identifier '{value}'")
        if any(ord(character) < 32 for character in value):
            raise DatabaseError(f"Invalid {identifier_type} identifier '{value}'")
        return value.strip()

    def _quote_identifier(self, value: str) -> str:
        return f"`{value.replace('`', '``')}`"
