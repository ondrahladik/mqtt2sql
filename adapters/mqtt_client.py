import logging
import time
from collections.abc import Callable
from threading import Lock

import paho.mqtt.client as mqtt

from core.models import MqttConfig


class ManagedMqttClient:
    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._client: mqtt.Client | None = None
        self._config: MqttConfig | None = None
        self._reconnect_interval = 5
        self._message_handler: Callable[[str, bytes], None] | None = None
        self._topics: tuple[str, ...] = ()
        self._lock = Lock()

    def configure(
        self,
        config: MqttConfig,
        reconnect_interval: int,
        message_handler: Callable[[str, bytes], None],
    ) -> None:
        self._config = config
        self._reconnect_interval = reconnect_interval
        self._message_handler = message_handler
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=config.client_id, clean_session=True)
        if config.username:
            client.username_pw_set(config.username, config.password)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.reconnect_delay_set(min_delay=reconnect_interval, max_delay=max(reconnect_interval * 2, reconnect_interval))
        self._client = client

    def connect(self) -> None:
        client = self._require_client()
        config = self._require_config()
        while True:
            try:
                client.connect(config.host, config.port)
                self._logger.info("Connected to MQTT broker %s:%s", config.host, config.port)
                return
            except OSError as error:
                self._logger.error("MQTT connection failed: %s", error)
                time.sleep(self._reconnect_interval)

    def subscribe(self, topics: tuple[str, ...]) -> None:
        self._topics = topics
        client = self._require_client()
        if client.is_connected():
            for topic in topics:
                client.subscribe(topic)
                self._logger.info("Subscribed to topic '%s'", topic)

    def loop_start(self) -> None:
        self._require_client().loop_start()

    def close(self) -> None:
        client = self._client
        if client is None:
            return
        if client.is_connected():
            client.disconnect()
        client.loop_stop()

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: object,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        if reason_code.is_failure:
            self._logger.error("MQTT connect failed with code %s", reason_code)
            return
        self._logger.info("MQTT connection established")
        self.subscribe(self._topics)

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: object,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        if disconnect_flags.is_disconnect_packet_from_server:
            self._logger.warning("MQTT disconnected by broker with code %s", reason_code)
        else:
            self._logger.warning("MQTT disconnected unexpectedly with code %s", reason_code)
        if reason_code.is_failure:
            self._attempt_reconnect()

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: object,
        message: mqtt.MQTTMessage,
    ) -> None:
        handler = self._message_handler
        if handler is None:
            return
        handler(message.topic, bytes(message.payload))

    def _attempt_reconnect(self) -> None:
        client = self._require_client()
        with self._lock:
            while not client.is_connected():
                try:
                    client.reconnect()
                    self._logger.info("Reconnected to MQTT broker")
                    return
                except OSError as error:
                    self._logger.error("MQTT reconnect failed: %s", error)
                    time.sleep(self._reconnect_interval)

    def _require_client(self) -> mqtt.Client:
        if self._client is None:
            raise RuntimeError("MQTT client is not configured")
        return self._client

    def _require_config(self) -> MqttConfig:
        if self._config is None:
            raise RuntimeError("MQTT config is not loaded")
        return self._config
