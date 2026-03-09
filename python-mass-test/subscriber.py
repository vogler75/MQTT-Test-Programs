#!/usr/bin/env python3
"""
MQTT Mass Subscriber Test - Python Version
Subscribes to messages from multiple concurrent clients from MQTT broker
"""

import paho.mqtt.client as mqtt
import time
import threading
import sys
from typing import Dict, List
import uuid
from config import Config
from metrics import GlobalMetrics


class MQTTSubscriber:
    def __init__(self, subscriber_id: int, config: Config, metrics: GlobalMetrics):
        self.subscriber_id = subscriber_id
        self.config = config
        self.metrics = metrics
        self.client_id = f"sub-{uuid.uuid4()}"
        self.running = True
        self.connected = False
        self.client = None
        self.subscribed = False
        self.subscription_phase = True
        self.topics_to_subscribe = []
        self.subscribe_index = 0
        self.subscribe_timer = None

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"Subscriber {self.subscriber_id + 1}: ✅ Connected to broker")
            self.connected = True
            self.metrics.set_connected(self.subscriber_id, True)
            self.subscription_phase = True
            self.subscribe_index = 0
            # Start subscribing to topics
            self._subscribe_next_topic()
        else:
            print(f"Subscriber {self.subscriber_id + 1}: ❌ Connection failed with code {rc}")
            self.connected = False
            self.metrics.set_connected(self.subscriber_id, False)

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print(f"Subscriber {self.subscriber_id + 1}: ⚠️  Unexpected disconnection: {rc}")
        self.connected = False
        self.subscribed = False
        self.metrics.set_connected(self.subscriber_id, False)

    def on_subscribe(self, client, userdata, mid, granted_qos):
        topic = self.topics_to_subscribe[self.subscribe_index] if self.subscribe_index < len(self.topics_to_subscribe) else "?"
        qos_str = f"QoS={granted_qos[0]}" if granted_qos else "QoS=unknown"
        print(f"Subscriber {self.subscriber_id + 1}: ✅ SUBACK for '{topic}' ({self.subscribe_index + 1}/{len(self.topics_to_subscribe)}) [{qos_str}]")
        self.subscribe_index += 1
        if self.subscribe_index >= len(self.topics_to_subscribe):
            print(f"Subscriber {self.subscriber_id + 1}: ✅✅ All {len(self.topics_to_subscribe)} subscriptions confirmed!")
            self.subscription_phase = False
            self.subscribed = True
        else:
            # Schedule next subscription with 100ms delay
            if self.subscribe_timer:
                self.subscribe_timer.cancel()
            self.subscribe_timer = threading.Timer(0.01, self._subscribe_next_topic)
            self.subscribe_timer.start()

    def on_message(self, client, userdata, msg):
        self.metrics.increment_received(self.subscriber_id)
        # Print first few messages to verify receipt
        if self.metrics.clients[self.subscriber_id].get_total_received() <= 3:
            print(f"Subscriber {self.subscriber_id + 1}: 📨 Received message on '{msg.topic}' (payload: {len(msg.payload)} bytes)")

    def _subscribe_next_topic(self):
        """Subscribe to next topic in list"""
        if self.subscribe_index < len(self.topics_to_subscribe):
            topic = self.topics_to_subscribe[self.subscribe_index]
            print(f"Subscriber {self.subscriber_id + 1}: 📬 Subscribing to '{topic}' ({self.subscribe_index + 1}/{len(self.topics_to_subscribe)})")
            self.client.subscribe(topic, qos=self.config.qos)

    def run(self):
        """Main subscriber loop"""
        while self.running:
            try:
                self.client = mqtt.Client(client_id=self.client_id)
                self.client.on_connect = self.on_connect
                self.client.on_disconnect = self.on_disconnect
                self.client.on_subscribe = self.on_subscribe
                self.client.on_message = self.on_message

                print(f"Subscriber {self.subscriber_id + 1}: Connecting to {self.config.broker_host}:{self.config.broker_port}")
                self.client.connect(self.config.broker_host, self.config.broker_port, keepalive=120)
                self.client.loop_start()

                # Generate topics to subscribe to
                self.topics_to_subscribe = self._generate_topics()
                print(f"Subscriber {self.subscriber_id + 1}: Generated {len(self.topics_to_subscribe)} topics to subscribe to:")
                if len(self.topics_to_subscribe) <= 5:
                    for topic in self.topics_to_subscribe:
                        print(f"  - {topic}")
                else:
                    print(f"  - {self.topics_to_subscribe[0]}")
                    print(f"  - {self.topics_to_subscribe[1]}")
                    print(f"  ... ({len(self.topics_to_subscribe) - 2} more)")
                print(f"Subscriber {self.subscriber_id + 1}: Starting subscription process...")

                # Wait for subscription
                timeout = time.time() + 30
                while self.running and (self.subscription_phase or self.connected):
                    if not self.connected:
                        break
                    if self.subscribed:
                        print(f"Subscriber {self.subscriber_id + 1}: Now receiving messages...")
                        break
                    if time.time() > timeout:
                        print(f"Subscriber {self.subscriber_id + 1}: ⚠️  Subscription timeout")
                        break
                    time.sleep(0.1)

                # Receiving loop
                while self.running and self.connected:
                    time.sleep(0.1)

                if not self.running:
                    break

                print(f"Subscriber {self.subscriber_id + 1}: Connection lost, retrying in 2 seconds...")
                self.client.loop_stop()
                time.sleep(2)

            except Exception as e:
                print(f"Subscriber {self.subscriber_id + 1}: ❌ Error: {e}")
                if self.client:
                    try:
                        self.client.loop_stop()
                    except:
                        pass
                self.metrics.set_connected(self.subscriber_id, False)
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
        use_wildcard = self.config.use_wildcard
        use_leafs = self.config.use_leafs

        def generate_topics_recursive(current_path: str, depth: int) -> List[str]:
            if depth == 0:
                return [current_path] if current_path else []

            result = []
            for i in range(topics_per_node):
                topic_name = f"{current_path}/{i:02d}" if current_path else f"{prefix}{self.subscriber_id + 1:05d}/{i:02d}"
                if depth == 1:
                    if use_wildcard and use_leafs:
                        # Wildcard at parent-of-leaf level
                        result.append(f"{topic_name}/#")
                    elif use_wildcard:
                        result.append(f"{topic_name}/#")
                    else:
                        result.append(topic_name)
                else:
                    result.extend(generate_topics_recursive(topic_name, depth - 1))
            return result

        if use_wildcard and not use_leafs:
            # Single wildcard at base level
            return [f"{prefix}{self.subscriber_id + 1:02d}/#"]

        return generate_topics_recursive("", max_depth)

    def stop(self):
        """Stop the subscriber"""
        self.running = False
        if self.subscribe_timer:
            self.subscribe_timer.cancel()
        if self.client:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except:
                pass


def main():
    """Main function to start multiple subscribers"""
    config = Config.load_or_default()
    metrics = GlobalMetrics(config.num_producers)

    print(f"\n📊 Starting {config.num_producers} subscribers...")

    # Create and start subscribers
    subscribers: List[MQTTSubscriber] = []
    threads: List[threading.Thread] = []

    for i in range(config.num_producers):
        sub = MQTTSubscriber(i, config, metrics)
        subscribers.append(sub)
        thread = threading.Thread(target=sub.run, daemon=True)
        thread.start()
        threads.append(thread)

    time.sleep(1)
    print("✅ All subscribers started!")
    print("📊 Subscribers running (press C to clear, Q to quit)...")

    try:
        while True:
            # Print metrics every second
            time.sleep(1)
            connected = metrics.get_connected_count()
            total = config.num_producers
            total_recv = metrics.get_total_received()
            vps = metrics.get_total_received_vps()
            print(f"📈 Connected: {connected}/{total} clients | Received: {total_recv} | v/s: {vps:.2f} | ▶️  Running")

    except KeyboardInterrupt:
        print("\n⏹️  Stopping subscribers...")
        for sub in subscribers:
            sub.stop()
        for thread in threads:
            thread.join(timeout=2)
        print("✅ Test completed!")
        print(f"Total messages received: {metrics.get_total_received()}")


if __name__ == "__main__":
    main()
