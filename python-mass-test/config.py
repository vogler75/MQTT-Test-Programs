#!/usr/bin/env python3
"""
Configuration module for MQTT test programs
"""

import json
import os
from typing import Optional


class Config:
    def __init__(self):
        self.broker_host = "localhost"
        self.broker_port = 1883
        self.num_producers = 10
        self.num_topics = 10
        self.topics_per_node = 3
        self.max_depth = 3
        self.sleep_ms = 1000
        self.qos = 0
        self.retained = False
        self.topic_prefix = "test"
        self.subscribe_percentage = 100
        self.use_leafs = False
        self.use_wildcard = False
        self.is_paused = False

    @classmethod
    def load_or_default(cls, config_file: Optional[str] = None) -> "Config":
        """Load config from file or return default"""
        config = cls()

        # Try to load from specified file
        if config_file:
            if os.path.exists(config_file):
                try:
                    with open(config_file, 'r') as f:
                        data = json.load(f)
                        for key, value in data.items():
                            if hasattr(config, key):
                                setattr(config, key, value)
                    print(f"✅ Loaded configuration from: {config_file}")
                    return config
                except Exception as e:
                    print(f"❌ Failed to load config from {config_file}: {e}")
                    return config

        # Try to load from default config.json
        if os.path.exists("config.json"):
            try:
                with open("config.json", 'r') as f:
                    data = json.load(f)
                    for key, value in data.items():
                        if hasattr(config, key):
                            setattr(config, key, value)
                print("✅ Loaded configuration from: config.json")
                return config
            except Exception as e:
                print(f"⚠️  Failed to load config.json: {e}")

        return config

    def save(self, config_file: str = "config.json"):
        """Save config to file"""
        try:
            data = {
                'broker_host': self.broker_host,
                'broker_port': self.broker_port,
                'num_producers': self.num_producers,
                'num_topics': self.num_topics,
                'topics_per_node': self.topics_per_node,
                'max_depth': self.max_depth,
                'sleep_ms': self.sleep_ms,
                'qos': self.qos,
                'retained': self.retained,
                'topic_prefix': self.topic_prefix,
                'subscribe_percentage': self.subscribe_percentage,
                'use_leafs': self.use_leafs,
                'use_wildcard': self.use_wildcard,
            }
            with open(config_file, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"✅ Configuration saved to: {config_file}")
        except Exception as e:
            print(f"❌ Failed to save configuration: {e}")

    def __repr__(self):
        return (
            f"Config(\n"
            f"  broker_host={self.broker_host},\n"
            f"  broker_port={self.broker_port},\n"
            f"  num_producers={self.num_producers},\n"
            f"  num_topics={self.num_topics},\n"
            f"  topics_per_node={self.topics_per_node},\n"
            f"  max_depth={self.max_depth},\n"
            f"  sleep_ms={self.sleep_ms},\n"
            f"  qos={self.qos},\n"
            f"  retained={self.retained},\n"
            f"  topic_prefix={self.topic_prefix},\n"
            f"  subscribe_percentage={self.subscribe_percentage},\n"
            f"  use_leafs={self.use_leafs},\n"
            f"  use_wildcard={self.use_wildcard}\n"
            f")"
        )
