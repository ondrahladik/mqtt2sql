import json
import logging
from collections.abc import Mapping

from domain.exceptions import DatabaseError
from domain.models import ConnectorConfig
from domain.ports import DatabaseGateway, MessageHandler


class JsonMessageProcessor(MessageHandler):
    def __init__(self, connectors: tuple[ConnectorConfig, ...], database: DatabaseGateway, logger: logging.Logger) -> None:
        self._connectors_by_topic = {connector.topic: connector for connector in connectors}
        self._database = database
        self._logger = logger

    def handle(self, topic: str, payload: bytes) -> None:
        connector = self._connectors_by_topic.get(topic)
        if connector is None:
            self._logger.warning("No connector configured for topic '%s'", topic)
            return
        parsed_payload = self._parse_payload(payload, connector.name)
        if parsed_payload is None:
            return
        mapped_payload = self._apply_mapping(parsed_payload, connector)
        if not mapped_payload:
            self._logger.warning("Skipping empty payload for connector '%s'", connector.name)
            return
        try:
            self._database.insert(connector, mapped_payload)
        except DatabaseError as error:
            self._logger.error("Failed to persist message for connector '%s': %s", connector.name, error)

    def _parse_payload(self, payload: bytes, connector_name: str) -> dict[str, object] | None:
        try:
            raw_value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._logger.warning("Ignoring invalid JSON payload for connector '%s'", connector_name)
            return None
        if not isinstance(raw_value, dict):
            self._logger.warning("Ignoring non-object JSON payload for connector '%s'", connector_name)
            return None
        return {str(key): self._normalize_value(value) for key, value in raw_value.items()}

    def _apply_mapping(self, payload: Mapping[str, object], connector: ConnectorConfig) -> dict[str, object]:
        if not connector.mapping:
            return dict(payload)
        return {connector.mapping.get(key, key): value for key, value in payload.items()}

    def _normalize_value(self, value: object) -> object:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return value
