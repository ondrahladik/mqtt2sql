import logging
import os
import secrets
from pathlib import Path
from typing import Any

import yaml
from flask import Flask, flash, redirect, render_template, request, url_for
from werkzeug.serving import make_server

from adapters.config_loader import save_yaml_file, validate_settings
from core.exceptions import ConfigurationError


def create_web_app(config_path: Path, topics_path: Path) -> Flask:
    app = Flask(__name__, template_folder="templates")
    app.config["SECRET_KEY"] = os.environ.get("MQTT2SQL_WEB_SECRET", secrets.token_hex(32))

    @app.get("/")
    def index() -> str:
        config_text = _read_text(config_path)
        topics_text = _read_text(topics_path)
        return render_template(
            "index.html",
            config_text=config_text,
            topics_text=topics_text,
            connectors=_extract_connectors(topics_text),
        )

    @app.post("/save")
    def save() -> str:
        config_text = request.form.get("config_text", "")
        topics_text = request.form.get("topics_text", "")
        try:
            config_data = _parse_yaml_text(config_text, config_path)
            topics_data = _parse_yaml_text(topics_text, topics_path)
            validate_settings(config_data, topics_data)
            save_yaml_file(config_path, config_data)
            save_yaml_file(topics_path, topics_data)
        except (ConfigurationError, yaml.YAMLError) as error:
            flash(str(error), "error")
            return render_template(
                "index.html",
                config_text=config_text,
                topics_text=topics_text,
                connectors=_extract_connectors(topics_text),
            )
        flash("Konfigurace byla uložena.", "success")
        return redirect(url_for("index"))

    @app.post("/reset")
    def reset() -> str:
        return redirect(url_for("index"))

    return app


def serve_web(config_path: Path, topics_path: Path, host: str, port: int) -> None:
    logger = logging.getLogger("mqtt2sql.web")
    app = create_web_app(config_path, topics_path)
    logger.info("Web interface listening on http://%s:%s", host, port)
    server = make_server(host=host, port=port, app=app, threaded=True)
    server.serve_forever()


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _parse_yaml_text(text: str, path: Path) -> dict[str, Any]:
    data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        raise ConfigurationError(f"Configuration file '{path}' must contain a YAML object")
    return data


def _extract_connectors(topics_text: str) -> list[dict[str, Any]]:
    try:
        data = yaml.safe_load(topics_text) or {}
    except yaml.YAMLError:
        return []
    connectors = data.get("connectors")
    if not isinstance(connectors, list):
        return []
    result: list[dict[str, Any]] = []
    for connector in connectors:
        if not isinstance(connector, dict):
            continue
        mapping = connector.get("mapping")
        result.append(
            {
                "name": str(connector.get("name", "")).strip(),
                "topic": str(connector.get("topic", "")).strip(),
                "table": str(connector.get("table", "")).strip(),
                "mapping": mapping if isinstance(mapping, dict) else {},
            }
        )
    return result
