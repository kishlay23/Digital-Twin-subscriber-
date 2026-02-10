"""MQTT subscriber that writes sensor data to Postgres."""

import json
import logging
import signal
import sys
import threading
import time
import paho.mqtt.client as mqtt

from config_loader import parse_ts, normalize_hw
from db import PostgresDB
from constants import ALLOWED_SENSOR_TYPES

logger = logging.getLogger(__name__)


class Subscriber:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.db = PostgresDB(cfg)

        self.hw_mapping = cfg.get("hardware_mapping", {})
        if not self.hw_mapping:
            logger.warning("No hardware_mapping found in config!")

        self.client = mqtt.Client(client_id="pg-subscriber", protocol=mqtt.MQTTv311)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def start(self) -> None:
        broker_host = self.cfg.get("mqtt", {}).get("host")
        broker_port = self.cfg.get("mqtt", {}).get("port", 1883)
        logger.info("Starting subscriber...")
        logger.info("Connecting to MQTT broker at %s:%s", broker_host, broker_port)
        # keep trying to connect until successful, with a delay between attempts
        while True:
            try:
                self.client.connect(broker_host, broker_port, 2000)
                break
            except Exception:
                logger.warning("Connection failed, retrying in 5 seconds...")
                time.sleep(5)

        logger.info("MQTT connection initiated")
        self.client.loop_start()
        logger.info("MQTT loop started")
        logger.info("Subscriber is running. Press Ctrl+C to stop.")
        logger.info("Listening for messages...")

        def stop(*args):
            logger.info("Stopping subscriber...")
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("MQTT disconnected")
            try:
                self.db.close()
            finally:
                sys.exit(0)

        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
        threading.Event().wait()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("MQTT Connected")
            topic = self.cfg.get("mqtt", {}).get("topic", "+/+/+/+/+")
            client.subscribe(topic, qos=1)
            logger.info("Subscribed to: %s", topic)
        else:
            logger.error("Connection failed rc=%s", rc)

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        logger.info("Received → %s", topic)

        try:
            data = json.loads(msg.payload.decode())
        except Exception:
            logger.exception("Invalid JSON payload")
            return

        parts = topic.split("/")
        hw_from_topic = parts[3] if len(parts) > 3 else None
        type_from_topic = parts[4] if len(parts) > 4 else None

        hardware_id = normalize_hw(data.get("hardware_id") or hw_from_topic)
        if not hardware_id:
            logger.error("No hardware_id found in message or topic")
            return

        sensor_type = (data.get("sensor_type") or type_from_topic or "").lower()
        if sensor_type not in ALLOWED_SENSOR_TYPES:
            logger.error("Invalid sensor type: %s", sensor_type)
            return

        hw_config = self.hw_mapping.get(hardware_id)
        if not hw_config:
            logger.error("No mapping found for hardware_id: %s", hardware_id)
            logger.debug("Available hardware_ids: %s", list(self.hw_mapping.keys()))
            return

        twin_short_name = hw_config.get("twin_short_name")
        zone_short_name = hw_config.get("zone_short_name")
        if not twin_short_name or not zone_short_name:
            logger.error("Invalid config for %s: %s", hardware_id, hw_config)
            return

        twin_id = self.db.get_twin_id(twin_short_name)
        if not twin_id:
            logger.error("Digital twin not found: %s", twin_short_name)
            return

        zone_id = self.db.get_zone_id(twin_id, zone_short_name)
        if not zone_id:
            logger.error(
                "Zone not found: %s in twin: %s", zone_short_name, twin_short_name
            )
            return

        value = data.get("value")
        if value is None:
            logger.error("No value found in message")
            return

        if isinstance(value, str):
            value = value.strip().replace("c", "").replace("C", "")

        try:
            value = float(value)
        except Exception:
            logger.exception("Invalid numeric value: %r", value)
            return

        ts = parse_ts(data.get("timestamp"))

        try:
            sensor_id = self.db.ensure_sensor(zone_id, sensor_type)
            if sensor_id is None:
                logger.error("Sensor id not available; aborting insert")
                return
            self.db.insert_sensor_data(sensor_type, sensor_id, value, ts)
            logger.info(
                "✓ Inserted: value=%s type=%s hw=%s twin=%s zone=%s",
                value,
                sensor_type,
                hardware_id,
                twin_short_name,
                zone_short_name,
            )
        except Exception:
            logger.exception("Database insert failed")
