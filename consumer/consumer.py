"""
consumer/consumer.py

Reads transactions from Kafka, scores them locally, writes results to Postgres.

Run:
    python consumer/consumer.py
"""

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Callable

import joblib
import psycopg2
import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaConnectionError
from psycopg2.pool import SimpleConnectionPool

load_dotenv()

logging.basicConfig(
    level=logging.getLevelName(os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_GROUP_ID: str = os.getenv("KAFKA_GROUP_ID", "fraud-consumer-group")
MODEL_PATH: Path = Path(os.getenv("MODEL_PATH", "model/model.pkl"))
FEATURE_COLUMNS_PATH: Path = Path(os.getenv("FEATURE_COLUMNS_PATH", "model/feature_columns.json"))

API_BASE_URL: str = os.getenv("API_BASE_URL", "http://localhost:8000")

_DB_DSN: str = (
    f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')} "
    f"dbname={os.getenv('POSTGRES_DB', 'frauddb')} "
    f"user={os.getenv('POSTGRES_USER', 'fraud_user')} "
    f"password={os.getenv('POSTGRES_PASSWORD', '')}"
)

# ── Model — loaded once at startup, not per message ───────────────
_model = joblib.load(MODEL_PATH)
_feature_columns: list[str] = json.loads(FEATURE_COLUMNS_PATH.read_text())


# ── Prediction functions ───────────────────────────────────────────

def predict_local(features: dict[str, float]) -> tuple[float, bool]:
    """Score using local model.pkl — zero network overhead, model tied to consumer process."""
    row = [[features.get(col, 0.0) for col in _feature_columns]]
    prob = float(_model.predict_proba(row)[0][1])
    return prob, prob >= 0.5


def predict_via_api(features: dict[str, float]) -> tuple[float, bool]:
    """
    Score via the FastAPI service over HTTP.
    Tradeoff vs predict_local:
      + Model version decoupled — update API without restarting consumer
      + API independently scalable and health-checked
      - Adds ~1–5ms network roundtrip per message
      - Consumer fails if API is down (add retry/circuit-breaker in production)
    """
    resp = requests.post(
        f"{API_BASE_URL}/predict",
        json=features,
        timeout=float(os.getenv("API_TIMEOUT_S", "5")),
    )
    resp.raise_for_status()
    data = resp.json()
    return data["fraud_probability"], data["is_fraud"]


# ── One-line swap: change predict_local → predict_via_api ─────────
PREDICT_FN: Callable[[dict[str, float]], tuple[float, bool]] = predict_via_api


# ── Postgres connection pool ───────────────────────────────────────

_pool: SimpleConnectionPool | None = None


def get_pool() -> SimpleConnectionPool:
    """Return the existing pool or create a new one (called on startup and after reconnect)."""
    global _pool
    if _pool is None:
        _pool = SimpleConnectionPool(minconn=1, maxconn=5, dsn=_DB_DSN)
        logger.info("Postgres pool initialised")
    return _pool


def write_prediction(
    transaction_id: str,
    features: dict,
    fraud_prob: float,
    is_fraud: bool,
    latency_ms: float,
) -> None:
    """
    Insert one scored row. Resets the pool on OperationalError so the
    next message triggers a fresh connection rather than dying silently.
    """
    global _pool
    pool = get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO predictions
                    (transaction_id, features, fraud_probability, is_fraud, latency_ms)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (transaction_id, json.dumps(features), fraud_prob, is_fraud, latency_ms),
            )
        conn.commit()
        pool.putconn(conn)
    except psycopg2.OperationalError as exc:
        logger.warning("Postgres connection lost — resetting pool: %s", exc)
        try:
            pool.putconn(conn, close=True)
        except Exception:
            pass
        _pool = None
        raise


# ── Message processing ─────────────────────────────────────────────

def process_message(msg_value: dict) -> None:
    """Validate, score, and persist a single Kafka message."""
    transaction_id: str | None = msg_value.get("transaction_id")
    features: dict | None = msg_value.get("features")

    if not transaction_id or not isinstance(features, dict):
        raise ValueError(f"Missing transaction_id or features. Got keys: {list(msg_value.keys())}")

    t0 = time.monotonic()
    fraud_prob, is_fraud = PREDICT_FN(features)
    latency_ms = (time.monotonic() - t0) * 1000

    write_prediction(transaction_id, features, fraud_prob, is_fraud, latency_ms)
    logger.debug(
        "txn=%.8s | prob=%.4f | fraud=%s | %.1fms",
        transaction_id, fraud_prob, is_fraud, latency_ms,
    )


# ── Main loop ─────────────────────────────────────────────────────

def run_consumer() -> None:
    """Subscribe to Kafka and process messages indefinitely."""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,          # enables offset checkpointing
            auto_offset_reset="earliest",     # catch up from last committed offset on restart
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
    except KafkaConnectionError as exc:
        logger.error("Cannot connect to Kafka: %s", exc)
        sys.exit(1)

    logger.info("Consumer ready | topic=%s | group=%s", KAFKA_TOPIC, KAFKA_GROUP_ID)
    processed = errors = 0

    for msg in consumer:
        try:
            process_message(msg.value)
            processed += 1
            if processed % 100 == 0:
                logger.info("Processed %d | errors %d", processed, errors)
        except ValueError as exc:
            errors += 1
            logger.warning("Malformed message at offset %d — skipping: %s", msg.offset, exc)
        except psycopg2.OperationalError:
            errors += 1
            logger.error("DB write failed at offset %d — skipping message", msg.offset)
        except Exception as exc:
            errors += 1
            logger.error("Unexpected error at offset %d: %s", msg.offset, exc)


if __name__ == "__main__":
    try:
        run_consumer()
    except KeyboardInterrupt:
        logger.info("Consumer stopped.")
        sys.exit(0)
