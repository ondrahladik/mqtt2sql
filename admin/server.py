import os
import secrets
from pathlib import Path
from typing import Any

from flask import Flask, flash, redirect, render_template, request, url_for
from werkzeug.serving import make_server

from adapters.config_loader import load_yaml_file, save_yaml_file, validate_settings
from core.exceptions import ConfigurationError


def create_web_app(config_path: Path, topics_path: Path) -> Flask:
    app = Flask(__name__, template_folder="templates")
    app.config["SECRET_KEY"] = os.environ.get("MQTT2SQL_WEB_SECRET", secrets.token_hex(32))

    @app.get("/")
    def index() -> str:
        return redirect(url_for("settings_page"))

    @app.get("/settings")
    def settings_page() -> str:
        config_data, topics_data = _load_raw_data(config_path, topics_path)
        return render_template(
            "index.html",
            page="settings",
            config_form=_config_to_form(config_data),
            connectors=_topics_to_connectors(topics_data),
            connector_form=None,
            connector_index=None,
            log_levels=_log_levels(),
        )

    @app.post("/settings")
    def save_settings() -> str:
        try:
            config_data, topics_data = _load_raw_data(config_path, topics_path)
            updated_config = _build_config_from_form(request.form)
            validate_settings(updated_config, topics_data)
            save_yaml_file(config_path, updated_config)
        except ConfigurationError as error:
            flash(str(error), "error")
            return render_template(
                "index.html",
                page="settings",
                config_form=_build_config_from_form(request.form, validate_numbers=False),
                connectors=_topics_to_connectors(topics_data if "topics_data" in locals() else {"connectors": []}),
                connector_form=None,
                connector_index=None,
                log_levels=_log_levels(),
            )
        flash("Settings saved.", "success")
        return redirect(url_for("settings_page"))

    @app.get("/connectors")
    def connectors_page() -> str:
        config_data, topics_data = _load_raw_data(config_path, topics_path)
        connectors = _topics_to_connectors(topics_data)
        connector_index = _parse_connector_index(request.args.get("selected"), len(connectors))
        connector_form = connectors[connector_index] if connectors else _empty_connector_form()
        return render_template(
            "index.html",
            page="connectors",
            config_form=_config_to_form(config_data),
            connectors=connectors,
            connector_form=connector_form,
            connector_index=connector_index,
            log_levels=_log_levels(),
        )

    @app.get("/connectors/new")
    def new_connector_page() -> str:
        config_data, topics_data = _load_raw_data(config_path, topics_path)
        return render_template(
            "index.html",
            page="connectors",
            config_form=_config_to_form(config_data),
            connectors=_topics_to_connectors(topics_data),
            connector_form=_empty_connector_form(),
            connector_index=None,
            log_levels=_log_levels(),
        )

    @app.post("/connectors/create")
    def create_connector() -> str:
        config_data, topics_data = _load_raw_data(config_path, topics_path)
        connectors = _topics_to_connectors(topics_data)
        connector_form = _build_connector_from_form(request.form)
        updated_connectors = [*connectors, connector_form]
        try:
            _save_topics(config_path, topics_path, config_data, updated_connectors)
        except ConfigurationError as error:
            flash(str(error), "error")
            return render_template(
                "index.html",
                page="connectors",
                config_form=_config_to_form(config_data),
                connectors=connectors,
                connector_form=connector_form,
                connector_index=None,
                log_levels=_log_levels(),
            )
        flash("Connector created.", "success")
        return redirect(url_for("connectors_page", selected=len(updated_connectors) - 1))

    @app.get("/connectors/<int:connector_index>/edit")
    def edit_connector_page(connector_index: int) -> str:
        config_data, topics_data = _load_raw_data(config_path, topics_path)
        connectors = _topics_to_connectors(topics_data)
        try:
            connector_form = _get_connector(connectors, connector_index)
        except ConfigurationError as error:
            flash(str(error), "error")
            return redirect(url_for("connectors_page"))
        return render_template(
            "index.html",
            page="connectors",
            config_form=_config_to_form(config_data),
            connectors=connectors,
            connector_form=connector_form,
            connector_index=connector_index,
            log_levels=_log_levels(),
        )

    @app.post("/connectors/<int:connector_index>/save")
    def save_connector(connector_index: int) -> str:
        config_data, topics_data = _load_raw_data(config_path, topics_path)
        connectors = _topics_to_connectors(topics_data)
        try:
            _get_connector(connectors, connector_index)
        except ConfigurationError as error:
            flash(str(error), "error")
            return redirect(url_for("connectors_page"))
        connector_form = _build_connector_from_form(request.form)
        updated_connectors = list(connectors)
        updated_connectors[connector_index] = connector_form
        try:
            _save_topics(config_path, topics_path, config_data, updated_connectors)
        except ConfigurationError as error:
            flash(str(error), "error")
            return render_template(
                "index.html",
                page="connectors",
                config_form=_config_to_form(config_data),
                connectors=connectors,
                connector_form=connector_form,
                connector_index=connector_index,
                log_levels=_log_levels(),
            )
        flash("Connector updated.", "success")
        return redirect(url_for("edit_connector_page", connector_index=connector_index))

    @app.post("/connectors/<int:connector_index>/delete")
    def delete_connector(connector_index: int) -> str:
        config_data, topics_data = _load_raw_data(config_path, topics_path)
        connectors = _topics_to_connectors(topics_data)
        try:
            _get_connector(connectors, connector_index)
        except ConfigurationError as error:
            flash(str(error), "error")
            return redirect(url_for("connectors_page"))
        updated_connectors = [connector for index, connector in enumerate(connectors) if index != connector_index]
        if not updated_connectors:
            flash("At least one connector is required.", "error")
            return redirect(url_for("edit_connector_page", connector_index=connector_index))
        try:
            _save_topics(config_path, topics_path, config_data, updated_connectors)
        except ConfigurationError as error:
            flash(str(error), "error")
            return redirect(url_for("edit_connector_page", connector_index=connector_index))
        flash("Connector deleted.", "success")
        return redirect(url_for("connectors_page"))

    return app


def serve_web(config_path: Path, topics_path: Path, host: str, port: int) -> None:
    app = create_web_app(config_path, topics_path)
    app.logger.info("Web interface listening on http://%s:%s", host, port)
    server = make_server(host=host, port=port, app=app, threaded=True)
    server.serve_forever()


def _load_raw_data(config_path: Path, topics_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    return load_yaml_file(config_path), load_yaml_file(topics_path)


def _config_to_form(config_data: dict[str, Any]) -> dict[str, Any]:
    mqtt_data = config_data.get("mqtt") if isinstance(config_data.get("mqtt"), dict) else {}
    mysql_data = config_data.get("mysql") if isinstance(config_data.get("mysql"), dict) else {}
    app_data = config_data.get("app") if isinstance(config_data.get("app"), dict) else {}
    return {
        "mqtt_host": str(mqtt_data.get("host", "")),
        "mqtt_port": str(mqtt_data.get("port", "")),
        "mqtt_username": str(mqtt_data.get("username", "")),
        "mqtt_password": str(mqtt_data.get("password", "")),
        "mqtt_client_id": str(mqtt_data.get("client_id", "")),
        "mysql_host": str(mysql_data.get("host", "")),
        "mysql_port": str(mysql_data.get("port", "")),
        "mysql_user": str(mysql_data.get("user", "")),
        "mysql_password": str(mysql_data.get("password", "")),
        "mysql_database": str(mysql_data.get("database", "")),
        "app_log_level": str(app_data.get("log_level", "INFO")).upper(),
        "app_reconnect_interval": str(app_data.get("reconnect_interval", "")),
    }


def _build_config_from_form(form: Any, validate_numbers: bool = True) -> dict[str, Any]:
    mqtt_port = _to_int(form.get("mqtt_port", ""), "mqtt.port", validate_numbers)
    mysql_port = _to_int(form.get("mysql_port", ""), "mysql.port", validate_numbers)
    reconnect_interval = _to_int(form.get("app_reconnect_interval", ""), "app.reconnect_interval", validate_numbers)
    return {
        "mqtt": {
            "host": _required_text(form.get("mqtt_host", ""), "mqtt.host"),
            "port": mqtt_port,
            "username": form.get("mqtt_username", "").strip(),
            "password": form.get("mqtt_password", "").strip(),
            "client_id": _required_text(form.get("mqtt_client_id", ""), "mqtt.client_id"),
        },
        "mysql": {
            "host": _required_text(form.get("mysql_host", ""), "mysql.host"),
            "port": mysql_port,
            "user": _required_text(form.get("mysql_user", ""), "mysql.user"),
            "password": form.get("mysql_password", ""),
            "database": _required_text(form.get("mysql_database", ""), "mysql.database"),
        },
        "app": {
            "log_level": _required_text(form.get("app_log_level", ""), "app.log_level").upper(),
            "reconnect_interval": reconnect_interval,
        },
    }


def _topics_to_connectors(topics_data: dict[str, Any]) -> list[dict[str, Any]]:
    connectors_data = topics_data.get("connectors")
    if not isinstance(connectors_data, list):
        return []
    connectors: list[dict[str, Any]] = []
    for connector in connectors_data:
        if not isinstance(connector, dict):
            continue
        mapping = connector.get("mapping")
        mapping_rows: list[dict[str, str]] = []
        if isinstance(mapping, dict):
            mapping_rows = [{"json_key": str(key), "column_name": str(value)} for key, value in mapping.items()]
        connectors.append(
            {
                "name": str(connector.get("name", "")).strip(),
                "topic": str(connector.get("topic", "")).strip(),
                "table": str(connector.get("table", "")).strip(),
                "mapping_rows": mapping_rows or [{"json_key": "", "column_name": ""}],
            }
        )
    return connectors


def _build_topics_data(connectors: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "connectors": [
            {
                "name": connector["name"],
                "topic": connector["topic"],
                "table": connector["table"],
                **({"mapping": connector["mapping"]} if connector["mapping"] else {}),
            }
            for connector in connectors
        ]
    }


def _build_connector_from_form(form: Any) -> dict[str, Any]:
    json_keys = form.getlist("mapping_json_key")
    column_names = form.getlist("mapping_column_name")
    mapping: dict[str, str] = {}
    mapping_rows: list[dict[str, str]] = []
    for json_key, column_name in zip(json_keys, column_names):
        left = json_key.strip()
        right = column_name.strip()
        mapping_rows.append({"json_key": left, "column_name": right})
        if left and right:
            mapping[left] = right
        elif left or right:
            raise ConfigurationError("Each mapping row must contain both JSON key and column name.")
    clean_rows = [row for row in mapping_rows if row["json_key"] or row["column_name"]]
    return {
        "name": _required_text(form.get("name", ""), "connector.name"),
        "topic": _required_text(form.get("topic", ""), "connector.topic"),
        "table": _required_text(form.get("table", ""), "connector.table"),
        "mapping": mapping,
        "mapping_rows": clean_rows or [{"json_key": "", "column_name": ""}],
    }


def _empty_connector_form() -> dict[str, Any]:
    return {
        "name": "",
        "topic": "",
        "table": "",
        "mapping": {},
        "mapping_rows": [{"json_key": "", "column_name": ""}],
    }


def _save_topics(
    config_path: Path,
    topics_path: Path,
    config_data: dict[str, Any],
    connector_forms: list[dict[str, Any]],
) -> None:
    topics_data = _build_topics_data(connector_forms)
    validate_settings(config_data, topics_data)
    save_yaml_file(topics_path, topics_data)


def _required_text(value: str, field_name: str) -> str:
    text = value.strip()
    if not text:
        raise ConfigurationError(f"Field '{field_name}' must not be empty")
    return text


def _to_int(value: str, field_name: str, validate_numbers: bool) -> int | str:
    text = value.strip()
    if not validate_numbers:
        return text
    if not text.isdigit():
        raise ConfigurationError(f"Field '{field_name}' must be a positive integer")
    number = int(text)
    if number <= 0:
        raise ConfigurationError(f"Field '{field_name}' must be a positive integer")
    return number


def _get_connector(connectors: list[dict[str, Any]], connector_index: int) -> dict[str, Any]:
    if connector_index < 0 or connector_index >= len(connectors):
        raise ConfigurationError("Connector does not exist")
    return connectors[connector_index]


def _parse_connector_index(value: str | None, total: int) -> int | None:
    if total == 0:
        return None
    if value is None:
        return 0
    if not value.isdigit():
        return 0
    index = int(value)
    if index < 0 or index >= total:
        return 0
    return index


def _log_levels() -> tuple[str, ...]:
    return ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
