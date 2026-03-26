from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class MqttConfig:
    host: str
    port: int
    username: str | None
    password: str | None
    client_id: str


@dataclass(frozen=True, slots=True)
class MysqlConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass(frozen=True, slots=True)
class AppConfig:
    log_level: str
    reconnect_interval: int


@dataclass(frozen=True, slots=True)
class ConnectorConfig:
    name: str
    topic: str
    table: str
    mapping: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TopicsConfig:
    connectors: tuple[ConnectorConfig, ...]
