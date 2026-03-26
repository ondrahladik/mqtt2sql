from pathlib import Path
from typing import Any

import yaml

from domain.exceptions import ConfigurationError
from domain.models import AppConfig, ConnectorConfig, MqttConfig, MysqlConfig, TopicsConfig


def load_settings(config_path: Path, topics_path: Path) -> tuple[MqttConfig, MysqlConfig, AppConfig, TopicsConfig]:
    config_data = _load_yaml(config_path)
    topics_data = _load_yaml(topics_path)
    mqtt_config = _parse_mqtt_config(config_data.get("mqtt"))
    mysql_config = _parse_mysql_config(config_data.get("mysql"))
    app_config = _parse_app_config(config_data.get("app"))
    topics_config = _parse_topics_config(topics_data)
    return mqtt_config, mysql_config, app_config, topics_config


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigurationError(f"Configuration file '{path}' does not exist")
    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}
    if not isinstance(data, dict):
        raise ConfigurationError(f"Configuration file '{path}' must contain a YAML object")
    return data


def _parse_mqtt_config(data: Any) -> MqttConfig:
    values = _require_dict(data, "mqtt")
    host = _require_non_empty_string(values.get("host"), "mqtt.host")
    port = _require_positive_int(values.get("port"), "mqtt.port")
    username = _optional_string(values.get("username"), "mqtt.username")
    password = _optional_string(values.get("password"), "mqtt.password")
    client_id = _require_non_empty_string(values.get("client_id"), "mqtt.client_id")
    return MqttConfig(host=host, port=port, username=username, password=password, client_id=client_id)


def _parse_mysql_config(data: Any) -> MysqlConfig:
    values = _require_dict(data, "mysql")
    host = _require_non_empty_string(values.get("host"), "mysql.host")
    port = _require_positive_int(values.get("port"), "mysql.port")
    user = _require_non_empty_string(values.get("user"), "mysql.user")
    password = _require_string(values.get("password"), "mysql.password")
    database = _require_non_empty_string(values.get("database"), "mysql.database")
    return MysqlConfig(host=host, port=port, user=user, password=password, database=database)


def _parse_app_config(data: Any) -> AppConfig:
    values = _require_dict(data, "app")
    log_level = _require_non_empty_string(values.get("log_level"), "app.log_level").upper()
    reconnect_interval = _require_positive_int(values.get("reconnect_interval"), "app.reconnect_interval")
    return AppConfig(log_level=log_level, reconnect_interval=reconnect_interval)


def _parse_topics_config(data: dict[str, Any]) -> TopicsConfig:
    connectors_raw = data.get("connectors")
    if not isinstance(connectors_raw, list) or not connectors_raw:
        raise ConfigurationError("topics.connectors must be a non-empty list")
    connectors: list[ConnectorConfig] = []
    names: set[str] = set()
    topics: set[str] = set()
    for index, connector_raw in enumerate(connectors_raw):
        connector_data = _require_dict(connector_raw, f"connectors[{index}]")
        name = _require_non_empty_string(connector_data.get("name"), f"connectors[{index}].name")
        topic = _require_non_empty_string(connector_data.get("topic"), f"connectors[{index}].topic")
        table = _require_non_empty_string(connector_data.get("table"), f"connectors[{index}].table")
        mapping_raw = connector_data.get("mapping") or {}
        mapping_data = _require_dict(mapping_raw, f"connectors[{index}].mapping")
        mapping = {
            _require_non_empty_string(key, f"connectors[{index}].mapping.key"): _require_non_empty_string(
                value, f"connectors[{index}].mapping[{key}]"
            )
            for key, value in mapping_data.items()
        }
        if name in names:
            raise ConfigurationError(f"Duplicate connector name '{name}'")
        if topic in topics:
            raise ConfigurationError(f"Duplicate connector topic '{topic}'")
        names.add(name)
        topics.add(topic)
        connectors.append(ConnectorConfig(name=name, topic=topic, table=table, mapping=mapping))
    return TopicsConfig(connectors=tuple(connectors))


def _require_dict(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"Field '{field_name}' must be an object")
    return value


def _require_non_empty_string(value: Any, field_name: str) -> str:
    result = _require_string(value, field_name)
    if not result.strip():
        raise ConfigurationError(f"Field '{field_name}' must not be empty")
    return result


def _require_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ConfigurationError(f"Field '{field_name}' must be a string")
    return value


def _optional_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_string(value, field_name)


def _require_positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigurationError(f"Field '{field_name}' must be a positive integer")
    return value
