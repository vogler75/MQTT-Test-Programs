# Python MQTT Mass Test Program

MQTT stress test tool written in Python using paho-mqtt client. Create multiple concurrent publisher and subscriber clients to test MQTT broker performance.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Publisher

Run multiple publishers:

```bash
python publisher.py
```

Controls:
- **P**: Pause/resume publishing
- **C**: Clear metrics
- **Q**: Quit

### Subscriber

Run multiple subscribers:

```bash
python subscriber.py
```

Controls:
- **C**: Clear metrics
- **Q**: Quit

## Configuration

Edit `config.json` to configure:

```json
{
  "broker_host": "localhost",
  "broker_port": 1883,
  "num_producers": 10,
  "num_topics": 10,
  "topics_per_node": 3,
  "max_depth": 3,
  "sleep_ms": 1000,
  "qos": 0,
  "retained": false,
  "topic_prefix": "test",
  "subscribe_percentage": 100,
  "use_leafs": false,
  "use_wildcard": false
}
```

### Configuration Options

- `broker_host`: MQTT broker hostname/IP
- `broker_port`: MQTT broker port (default: 1883)
- `num_producers`: Number of concurrent publisher clients
- `num_topics`: Total number of topics to generate
- `topics_per_node`: Topics per hierarchy level
- `max_depth`: Maximum topic hierarchy depth
- `sleep_ms`: Milliseconds between publishes
- `qos`: MQTT QoS level (0, 1, or 2)
- `retained`: Publish as retained messages
- `topic_prefix`: Topic prefix (e.g., "test")
- `subscribe_percentage`: Percentage of topics to subscribe to (0-100)
- `use_leafs`: Subscribe/publish only to leaf-level topics
- `use_wildcard`: Use wildcard subscriptions

## Features

- Multiple concurrent MQTT clients
- Automatic reconnection on connection loss
- Real-time metrics (messages/second, connected clients)
- Connection state tracking
- Pause/resume publishing
- Clear metrics counter
- Thread-based async I/O
- Configuration file support
- JSON payloads with timestamp and counter

## Topic Hierarchy

Topics are generated based on config:
- Prefix: `test00001` (for client 1)
- Format: `test00001/00000/00000/...` (based on depth)
- Leaf topics: Deepest level topics
- Wildcard: `test00001/#` for all topics under prefix

## Metrics

Real-time metrics printed every second:
- Connected clients count
- Total messages published/received
- Messages per second (v/s)
- Current status (Running/Paused)
