import argparse
import logging
import threading
from pathlib import Path

from adapters.config_loader import load_settings
from adapters.database import MysqlDatabaseGateway
from adapters.mqtt_client import ManagedMqttClient
from admin.server import serve_web
from core.exceptions import ConfigurationError, DatabaseError
from runtime.logging_utils import configure_logging
from service.message_processor import JsonMessageProcessor
from service.runner import Application


def main() -> int:
    args = _parse_args()
    if args.command == "web":
        return _run_web(args)
    if args.command == "all":
        return _run_all(args)
    return _run_service(args)


def _run_service(args: argparse.Namespace) -> int:
    try:
        mqtt_config, mysql_config, app_config, topics_config = load_settings(args.config, args.topics)
        logger = configure_logging(app_config.log_level)
    except (ConfigurationError, ValueError) as error:
        configure_logging("ERROR")
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
    parser.add_argument("--config", type=Path, default=Path("config/config.yaml"))
    parser.add_argument("--topics", type=Path, default=Path("config/topics.yaml"))
    subparsers = parser.add_subparsers(dest="command")
    parser.set_defaults(command="all", host="0.0.0.0", port=8080)
    subparsers.add_parser("run")
    web_parser = subparsers.add_parser("web")
    web_parser.add_argument("--host", default="0.0.0.0")
    web_parser.add_argument("--port", type=int, default=8080)
    all_parser = subparsers.add_parser("all")
    all_parser.add_argument("--host", default="0.0.0.0")
    all_parser.add_argument("--port", type=int, default=8080)
    return parser.parse_args()

def _run_web(args: argparse.Namespace) -> int:
    configure_logging(_resolve_web_log_level(args))
    serve_web(args.config, args.topics, args.host, args.port)
    return 0


def _run_all(args: argparse.Namespace) -> int:
    configure_logging(_resolve_web_log_level(args))
    web_thread = threading.Thread(target=serve_web, args=(args.config, args.topics, args.host, args.port), daemon=True)
    web_thread.start()
    return _run_service(args)


def _resolve_web_log_level(args: argparse.Namespace) -> str:
    try:
        _, _, app_config, _ = load_settings(args.config, args.topics)
        return app_config.log_level
    except ConfigurationError:
        return "INFO"
