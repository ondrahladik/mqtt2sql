from collections.abc import Mapping
from typing import Protocol

from core.models import ConnectorConfig


class DatabaseGateway(Protocol):
    def insert(self, connector: ConnectorConfig, payload: Mapping[str, object]) -> None:
        ...

    def close(self) -> None:
        ...


class MessageHandler(Protocol):
    def handle(self, topic: str, payload: bytes) -> None:
        ...
