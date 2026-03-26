# mqtt2sql Architecture

## Directory Layout

```text
mqtt2sql/
├── mqtt2sql.py
├── config/
├── docs/
├── core/
├── service/
├── adapters/
├── admin/
└── runtime/
```

## Layers

### core

Core contains the domain-level structures:

- data models
- exceptions
- ports and interfaces

### service

Service contains application behavior:

- message processing
- application runner lifecycle

### adapters

Adapters contain infrastructure integrations:

- MQTT client
- database gateway
- YAML configuration loader and saver

### admin

Admin contains the Flask web interface:

- route handling
- web forms
- HTML templates

### runtime

Runtime contains startup logic:

- CLI handling
- logging setup
- combined service and web startup

## Runtime Flow

1. CLI loads `config/config.yaml` and `config/topics.yaml`
2. logging is configured
3. MQTT and database adapters are created
4. the application subscribes to all configured topics
5. incoming messages are mapped and inserted into the target table
6. the web admin can update YAML configuration while the service is running

## Design Goals

- clean separation of concerns
- production-oriented structure
- YAML-based configuration
- easy future extension
- safe SQL insertion with parameter binding
