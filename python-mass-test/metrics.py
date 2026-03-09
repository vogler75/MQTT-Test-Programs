#!/usr/bin/env python3
"""
Metrics tracking for MQTT test programs
"""

import time
import threading
from typing import List


class ClientMetrics:
    def __init__(self, client_id: int):
        self.id = client_id
        self.total_published = 0
        self.total_received = 0
        self.connected = False
        self.lock = threading.Lock()

        # VPS tracking
        self.last_pub_vps_time = time.time()
        self.last_pub_vps_count = 0
        self.cached_pub_vps = 0.0

        self.last_recv_vps_time = time.time()
        self.last_recv_vps_count = 0
        self.cached_recv_vps = 0.0

    def increment_published(self):
        with self.lock:
            self.total_published += 1

    def increment_received(self):
        with self.lock:
            self.total_received += 1

    def get_total_published(self) -> int:
        with self.lock:
            return self.total_published

    def get_total_received(self) -> int:
        with self.lock:
            return self.total_received

    def calculate_vps(self) -> float:
        """Calculate published messages per second"""
        with self.lock:
            now = time.time()
            last_time = self.last_pub_vps_time

            if now <= last_time:
                return self.cached_pub_vps

            current_count = self.total_published
            last_count = self.last_pub_vps_count

            time_delta = now - last_time
            if time_delta == 0:
                return 0.0

            count_delta = current_count - last_count
            vps = count_delta / time_delta

            # Update for next check
            self.last_pub_vps_time = now
            self.last_pub_vps_count = current_count
            self.cached_pub_vps = vps

            return vps

    def calculate_received_vps(self) -> float:
        """Calculate received messages per second"""
        with self.lock:
            now = time.time()
            last_time = self.last_recv_vps_time

            if now <= last_time:
                return self.cached_recv_vps

            current_count = self.total_received
            last_count = self.last_recv_vps_count

            time_delta = now - last_time
            if time_delta == 0:
                return 0.0

            count_delta = current_count - last_count
            vps = count_delta / time_delta

            # Update for next check
            self.last_recv_vps_time = now
            self.last_recv_vps_count = current_count
            self.cached_recv_vps = vps

            return vps

    def set_connected(self, connected: bool):
        with self.lock:
            self.connected = connected

    def is_connected(self) -> bool:
        with self.lock:
            return self.connected

    def reset(self):
        """Reset all metrics"""
        with self.lock:
            self.total_published = 0
            self.total_received = 0
            now = time.time()
            self.last_pub_vps_time = now
            self.last_pub_vps_count = 0
            self.cached_pub_vps = 0.0
            self.last_recv_vps_time = now
            self.last_recv_vps_count = 0
            self.cached_recv_vps = 0.0


class GlobalMetrics:
    def __init__(self, num_clients: int):
        self.clients: List[ClientMetrics] = [ClientMetrics(i) for i in range(num_clients)]
        self.lock = threading.Lock()

    def increment_published(self, client_id: int):
        if 0 <= client_id < len(self.clients):
            self.clients[client_id].increment_published()

    def increment_received(self, client_id: int):
        if 0 <= client_id < len(self.clients):
            self.clients[client_id].increment_received()

    def set_connected(self, client_id: int, connected: bool):
        if 0 <= client_id < len(self.clients):
            self.clients[client_id].set_connected(connected)

    def get_connected_count(self) -> int:
        """Get number of connected clients"""
        return sum(1 for c in self.clients if c.is_connected())

    def get_total_published(self) -> int:
        """Get total published messages across all clients"""
        return sum(c.get_total_published() for c in self.clients)

    def get_total_received(self) -> int:
        """Get total received messages across all clients"""
        return sum(c.get_total_received() for c in self.clients)

    def get_total_vps(self) -> float:
        """Get total published messages per second"""
        return sum(c.calculate_vps() for c in self.clients)

    def get_total_received_vps(self) -> float:
        """Get total received messages per second"""
        return sum(c.calculate_received_vps() for c in self.clients)

    def reset(self):
        """Reset all metrics"""
        for client in self.clients:
            client.reset()
