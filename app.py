import argparse
import logging
import sys
from pathlib import Path

from application.message_processor import JsonMessageProcessor
from application.service import Application
from domain.exceptions import ConfigurationError, DatabaseError
from infrastructure.config_loader import load_settings
from infrastructure.database import MysqlDatabaseGateway
from infrastructure.mqtt_client import ManagedMqttClient


def main() -> int:
    args = _parse_args()
    try:
        mqtt_config, mysql_config, app_config, topics_config = load_settings(args.config, args.topics)
        logger = _configure_logging(app_config.log_level)
    except ConfigurationError as error:
        logging.basicConfig(level=logging.ERROR, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        logging.getLogger("mqtt2sql").error("Configuration error: %s", error)
        return 1
    database = MysqlDatabaseGateway(mysql_config, app_config, logger.getChild("database"))
    processor = JsonMessageProcessor(topics_config.connectors, database, logger.getChild("processor"))
    mqtt_client = ManagedMqttClient(logger.getChild("mqtt"))
    application = Application(
        mqtt_config=mqtt_config,
        app_config=app_config,
        connectors=topics_config.connectors,
        mqtt_client=mqtt_client,
        processor=processor,
        logger=logger,
    )
    try:
        return application.run()
    except (DatabaseError, OSError, RuntimeError) as error:
        logger.exception("Application failed: %s", error)
        return 1
    finally:
        database.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="mqtt2sql")
    parser.add_argument("--config", type=Path, default=Path("config.yaml"))
    parser.add_argument("--topics", type=Path, default=Path("topics.yaml"))
    return parser.parse_args()


def _configure_logging(log_level: str) -> logging.Logger:
    level = getattr(logging, log_level.upper(), None)
    if not isinstance(level, int):
        raise ConfigurationError(f"Unsupported log level '{log_level}'")
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s", stream=sys.stdout)
    return logging.getLogger("mqtt2sql")
