#!/usr/bin/env python3
"""
MQTT Mass Publisher Test - Python Version
Publishes messages from multiple concurrent clients to MQTT broker
"""

import paho.mqtt.client as mqtt
import json
import time
import threading
import sys
from datetime import datetime
from typing import Dict, List
import uuid
from config import Config
from metrics import GlobalMetrics


class MQTTPublisher:
    def __init__(self, publisher_id: int, config: Config, metrics: GlobalMetrics):
        self.publisher_id = publisher_id
        self.config = config
        self.metrics = metrics
        self.client_id = f"pub-{uuid.uuid4()}"
        self.running = True
        self.connected = False
        self.client = None
        self.publish_count = 0
        self.last_time = time.time()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"Publisher {self.publisher_id + 1}: ✅ Connected to broker")
            self.connected = True
            self.metrics.set_connected(self.publisher_id, True)
        else:
            print(f"Publisher {self.publisher_id + 1}: ❌ Connection failed with code {rc}")
            self.connected = False
            self.metrics.set_connected(self.publisher_id, False)

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print(f"Publisher {self.publisher_id + 1}: ⚠️  Unexpected disconnection: {rc}")
        self.connected = False
        self.metrics.set_connected(self.publisher_id, False)

    def on_publish(self, client, userdata, mid):
        self.metrics.increment_published(self.publisher_id)

    def run(self):
        """Main publisher loop"""
        while self.running:
            try:
                self.client = mqtt.Client(client_id=self.client_id)
                self.client.on_connect = self.on_connect
                self.client.on_disconnect = self.on_disconnect
                self.client.on_publish = self.on_publish

                print(f"Publisher {self.publisher_id + 1}: Connecting to {self.config.broker_host}:{self.config.broker_port}")
                self.client.connect(self.config.broker_host, self.config.broker_port, keepalive=120)
                self.client.loop_start()

                # Wait for connection
                timeout = time.time() + 10
                while not self.connected and time.time() < timeout and self.running:
                    time.sleep(0.1)

                if not self.connected:
                    print(f"Publisher {self.publisher_id + 1}: ❌ Failed to connect, retrying...")
                    self.client.loop_stop()
                    time.sleep(2)
                    continue

                # Publishing loop
                topic_index = 0
                topics = self._generate_topics()
                publish_interval = self.config.sleep_ms / 1000.0

                last_publish_time = time.time()
                while self.running and self.connected:
                    current_time = time.time()
                    if current_time - last_publish_time >= publish_interval:
                        if self.config.is_paused:
                            last_publish_time = current_time
                            continue

                        topic = topics[topic_index % len(topics)]
                        payload = json.dumps({
                            "ts": datetime.utcnow().isoformat(),
                            "counter": self.publish_count,
                            "value": round(time.time() % 1, 4)
                        })

                        self.client.publish(topic, payload, qos=self.config.qos, retain=self.config.retained)
                        self.publish_count += 1
                        topic_index += 1
                        last_publish_time = current_time

                    time.sleep(0.01)  # Small sleep to prevent busy loop

                if not self.running:
                    break

                print(f"Publisher {self.publisher_id + 1}: Connection lost, retrying in 2 seconds...")
                self.client.loop_stop()
                time.sleep(2)

            except Exception as e:
                print(f"Publisher {self.publisher_id + 1}: ❌ Error: {e}")
                if self.client:
                    try:
                        self.client.loop_stop()
                    except:
                        pass
                self.metrics.set_connected(self.publisher_id, False)
                if self.running:
                    time.sleep(2)

        if self.client:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except:
                pass

    def _generate_topics(self) -> List[str]:
        """Generate topic list based on config"""
        topics = []
        prefix = self.config.topic_prefix
        topics_per_node = self.config.topics_per_node
        max_depth = self.config.max_depth

        def generate_topics_recursive(current_path: str, depth: int) -> List[str]:
            if depth == 0:
                return [current_path] if current_path else []

            result = []
            for i in range(topics_per_node):
                topic_name = f"{current_path}/{i:02d}" if current_path else f"{prefix}{self.publisher_id + 1:05d}/{i:02d}"
                if depth == 1:
                    result.append(topic_name)
                else:
                    result.extend(generate_topics_recursive(topic_name, depth - 1))
            return result

        return generate_topics_recursive("", max_depth)

    def stop(self):
        """Stop the publisher"""
        self.running = False
        if self.client:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except:
                pass


def main():
    """Main function to start multiple publishers"""
    config = Config.load_or_default()
    metrics = GlobalMetrics(config.num_producers)

    print(f"\n📊 Starting {config.num_producers} publishers...")

    # Create and start publishers
    publishers: List[MQTTPublisher] = []
    threads: List[threading.Thread] = []

    for i in range(config.num_producers):
        pub = MQTTPublisher(i, config, metrics)
        publishers.append(pub)
        thread = threading.Thread(target=pub.run, daemon=True)
        thread.start()
        threads.append(thread)

    time.sleep(1)
    print("✅ All publishers started!")
    print("📊 Publishers running (press P to pause/resume, C to clear, Q to quit)...")

    try:
        while True:
            # Print metrics every second
            time.sleep(1)
            connected = metrics.get_connected_count()
            total = config.num_producers
            total_pub = metrics.get_total_published()
            vps = metrics.get_total_vps()
            status = "⏸️  PAUSED" if config.is_paused else "▶️  Running"
            print(f"📈 Connected: {connected}/{total} clients | Published: {total_pub} | v/s: {vps:.2f} | {status}")

    except KeyboardInterrupt:
        print("\n⏹️  Stopping publishers...")
        config.is_paused = False
        for pub in publishers:
            pub.stop()
        for thread in threads:
            thread.join(timeout=2)
        print("✅ Test completed!")
        print(f"Total messages published: {metrics.get_total_published()}")


if __name__ == "__main__":
    main()
