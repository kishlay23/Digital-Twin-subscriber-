"""PostgresDB wrapper with safe SQL identifier composition."""

import logging
from typing import Optional
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as _connection
from config_loader import read_config  # optional usage in tests
from constants import get_table_and_column

logger = logging.getLogger(__name__)


class PostgresDB:
    def __init__(self, cfg: dict):
        db = cfg.get("database", {})

        try:
            self.conn: _connection = psycopg2.connect(
                host=db.get("host", "10.165.23.54"),
                port=int(db.get("port", 5432)),
                user=db.get("user", "postgres"),
                password=db.get("password", "admin"),
                dbname=db.get("name", "DigitalTwin"),
            )
            self.conn.autocommit = True
            logger.info("Connected to Postgres database")
        except psycopg2.OperationalError:
            logger.exception("ERROR: Postgres connection/auth failed")
            raise

    def close(self) -> None:
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            logger.exception("Error closing Postgres connection")

    def get_twin_id(self, twin_short_name: str) -> Optional[int]:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT twin_id FROM digital_twins WHERE twin_short_name = %s AND status = true LIMIT 1",
                (twin_short_name,),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def get_zone_id(self, twin_id: int, zone_short_name: str) -> Optional[int]:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT zone_id FROM zones WHERE twin_id = %s AND zone_short_name = %s LIMIT 1",
                (twin_id, zone_short_name),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def ensure_sensor(self, zone_id: int, sensor_type: str) -> Optional[int]:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT sensor_id FROM sensors WHERE zone_id = %s AND sensor_type = %s LIMIT 1",
                (zone_id, sensor_type),
            )
            row = cur.fetchone()
            if row:
                return row[0]

            logger.error(
                "Sensor not found: zone_id=%s sensor_type=%s", zone_id, sensor_type
            )
            return None

    def insert_sensor_data(
        self, sensor_type: str, sensor_id: int, value: float, ts
    ) -> None:
        table_name, column_name = get_table_and_column(sensor_type)
        query = sql.SQL(
            "INSERT INTO {table} (sensor_id, {col}, created_date) VALUES (%s, %s, %s)"
        ).format(
            table=sql.Identifier(table_name),
            col=sql.Identifier(column_name),
        )
        with self.conn.cursor() as cur:
            cur.execute(query, (sensor_id, value, ts))
            logger.info(
                "Inserted sensor data into %s: sensor_id=%s value=%s ts=%s",
                table_name,
                sensor_id,
                value,
                ts,
            )
