# mqtt2sql Usage

## Overview

`mqtt2sql` is a long-running Python service that listens on MQTT topics and stores incoming JSON payloads into MySQL or MariaDB tables.

The project includes:

- the MQTT to SQL service
- a web administration interface
- YAML configuration storage

## Requirements

- Python 3.12
- MySQL or MariaDB
- access to an MQTT broker

## Installation

Install dependencies:

```bash
pip install -r requirements.txt
```

## Start

Start the service and the web interface together:

```bash
py mqtt2sql.py
```

The default web address is:

```text
http://127.0.0.1:8000
```

Start only the MQTT service:

```bash
py mqtt2sql.py run
```

Start only the web interface:

```bash
py mqtt2sql.py web
```

## Configuration Files

Configuration files are stored in:

- `config/config.yaml`
- `config/topics.yaml`

Sample files are stored in:

- `config/config.yaml.sample`
- `config/topics.yaml.sample`

## config.yaml Structure

```yaml
mqtt:
  host: localhost
  port: 1883
  username: mqtt_user
  password: mqtt_password
  client_id: mqtt2sql

mysql:
  host: localhost
  port: 3306
  user: mqtt2sql
  password: mysql_password
  database: mqtt2sql

app:
  log_level: INFO
  reconnect_interval: 5
```

## topics.yaml Structure

```yaml
connectors:
  - name: sensor_living_room
    topic: home/living-room/sensor
    table: living_room_measurements
    mapping:
      temperature: temperature
      humidity: humidity
```

## Web Admin

The web interface provides two main sections:

### General Settings

This section lets you manage:

- MQTT connection settings
- MySQL or MariaDB settings
- application log level
- reconnect interval

### Connectors

This section lets you:

- create a connector
- edit an existing connector
- delete a connector
- manage optional mapping rows

Each connector contains:

- connector name
- MQTT topic
- destination database table
- optional mapping from JSON keys to database column names

## Message Processing

When a message is received:

1. the payload is parsed as JSON
2. the payload must be a JSON object
3. mapping is applied when defined
4. an SQL `INSERT` statement is generated dynamically
5. if the destination table contains `created_at`, it is filled automatically with the current server time

Invalid JSON payloads are ignored.

## Logging

The terminal output uses colored log levels for easier reading.

Supported levels:

- `DEBUG`
- `INFO`
- `WARNING`
- `ERROR`
- `CRITICAL`

The log level is controlled from `config/config.yaml` or the web admin.

## Notes

- Connector names and topics must be unique.
- At least one connector must remain configured.
- Database inserts use parameterized queries.
- The web admin writes changes back to YAML files.
