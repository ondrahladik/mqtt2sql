import logging
import signal
import threading
from types import FrameType

from adapters.mqtt_client import ManagedMqttClient
from core.models import AppConfig, ConnectorConfig, MqttConfig
from service.message_processor import JsonMessageProcessor


class Application:
    def __init__(
        self,
        mqtt_config: MqttConfig,
        app_config: AppConfig,
        connectors: tuple[ConnectorConfig, ...],
        mqtt_client: ManagedMqttClient,
        processor: JsonMessageProcessor,
        logger: logging.Logger,
    ) -> None:
        self._mqtt_config = mqtt_config
        self._app_config = app_config
        self._connectors = connectors
        self._mqtt_client = mqtt_client
        self._processor = processor
        self._logger = logger
        self._shutdown_event = threading.Event()

    def run(self) -> int:
        self._install_signal_handlers()
        self._mqtt_client.configure(self._mqtt_config, self._app_config.reconnect_interval, self._processor.handle)
        self._mqtt_client.connect()
        self._mqtt_client.subscribe(tuple(connector.topic for connector in self._connectors))
        self._mqtt_client.loop_start()
        self._logger.info("mqtt2sql started")
        self._shutdown_event.wait()
        self._logger.info("mqtt2sql stopping")
        self._mqtt_client.close()
        self._logger.info("mqtt2sql stopped")
        return 0

    def stop(self) -> None:
        self._shutdown_event.set()

    def _install_signal_handlers(self) -> None:
        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, self._handle_signal)

    def _handle_signal(self, signum: int, frame: FrameType | None) -> None:
        self._logger.info("Received signal %s", signum)
        self.stop()
