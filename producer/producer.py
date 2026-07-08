"""
producer/producer.py

Reads PaySim transactions row-by-row and publishes JSON messages to
the Kafka `transactions` topic with realistic inter-message pacing.

Kafka concept — Producer:
    A Kafka producer is a client that appends records to a topic.
    Records are durably stored by the broker (Kafka server) and can be
    consumed independently by one or many consumers. The producer never
    needs to know who (or how many) consumers exist.

Schema note — V1-V28 vs PaySim fields:
    The LightGBM model was trained on creditcard.csv, where V1-V28 are
    PCA-anonymised components of undisclosed original features. PaySim
    has raw financial fields (amount, balance deltas, etc.) with no
    correspondence to PCA components.

    Resolution: `Amount` is mapped directly; V1-V28 are set to 0.0 and
    labelled as placeholders. In production you would either retrain on
    PaySim features or apply the same PCA transform used on creditcard.
    PaySim's `isFraud` label is forwarded as message metadata so the
    dashboard can compare ground-truth against model probability.

Run:
    python producer/producer.py
"""

import csv
import json
import logging
import os
import random
import sys
import time
import uuid
from pathlib import Path
from typing import Generator

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaConnectionError, KafkaError

load_dotenv()

logging.basicConfig(
    level=logging.getLevelName(os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "transactions")
PAYSIM_PATH: Path = Path(os.getenv("PAYSIM_PATH", "data/paysim.csv"))
FEATURE_COLUMNS_PATH: Path = Path(os.getenv("FEATURE_COLUMNS_PATH", "model/feature_columns.json"))
MIN_DELAY: float = float(os.getenv("PRODUCER_MIN_DELAY", "0.1"))
MAX_DELAY: float = float(os.getenv("PRODUCER_MAX_DELAY", "1.0"))
LOG_INTERVAL: int = int(os.getenv("PRODUCER_LOG_INTERVAL", "100"))
MAX_RETRIES: int = int(os.getenv("KAFKA_MAX_RETRIES", "5"))


def load_feature_columns(path: Path) -> list[str]:

    if not path.exists():
        raise FileNotFoundError(
            f"Feature schema not found at '{path}'. "
            "Run `python model/train.py` first to generate it."
        )
    columns: list[str] = json.loads(path.read_text())
    logger.info("Loaded feature schema: %d columns", len(columns))
    return columns


def build_producer(retries: int = MAX_RETRIES) -> KafkaProducer:
    """
    Create a KafkaProducer with exponential-backoff retry on connection failure.

    Separating connection setup from the send loop means Kafka being
    temporarily down at startup produces a clear, retryable error rather
    than a cryptic failure inside a hot loop.
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    
                acks="all",
                retries=3,
                # Limit send buffer size to avoid unbounded memory growth if
                # Kafka slows down and messages queue up in the producer
                max_block_ms=10_000,
            )
            logger.info("Connected to Kafka broker at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except KafkaConnectionError:
            wait = 2 ** attempt 
            logger.warning(
                "Kafka not reachable (attempt %d/%d) — retrying in %ds. "
                "Is Docker running? Try: docker compose up -d",
                attempt, retries, wait,
            )
            if attempt >= retries:
                raise
            time.sleep(wait)

    raise RuntimeError("Failed to connect to Kafka after all retries")


def map_paysim_to_message(
    row: dict[str, str],
    feature_columns: list[str],
    transaction_id: str,
) -> dict:
    """
    Build a Kafka message from a raw PaySim CSV row.

    Message structure:
        transaction_id : str   — UUID assigned by the producer
        features       : dict  — exactly the columns the model expects
        meta           : dict  — PaySim fields kept for dashboard/monitoring

    V1-V28 are PCA placeholders (0.0). See module docstring for why.
    Amount is the only directly mappable financial field.
    """
    # Start with all model features zeroed — explicit default, not silent omission
    features: dict[str, float] = {col: 0.0 for col in feature_columns}
    if "Amount" in features:
        features["Amount"] = float(row.get("amount", 0.0))

    return {
        "transaction_id": transaction_id,
        "features": features,
        # Metadata travels with the message but is NOT fed to the model.
        # The consumer stores it in PostgreSQL for the dashboard to surface.
        "meta": {
            "paysim_step": int(row.get("step", 0)),
            "paysim_type": row.get("type", "UNKNOWN"),
            "paysim_amount": float(row.get("amount", 0.0)),
            "name_orig": row.get("nameOrig", ""),
            "name_dest": row.get("nameDest", ""),
            "ground_truth_fraud": int(row.get("isFraud", 0)),
        },
    }


def stream_csv_rows(path: Path) -> Generator[dict[str, str], None, None]:
    """
    Yield CSV rows one at a time using a generator.

    Generator-based iteration means the full 6M-row PaySim file never
    loads into memory — each row is processed and discarded before the
    next is read. Memory usage stays constant regardless of file size.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"PaySim dataset not found at '{path}'.\n"
            "Download from: kaggle.com/datasets/ealaxi/paysim1\n"
            "Place at: data/paysim.csv"
        )
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield row


def run_producer() -> None:
    """
    Main loop: stream PaySim rows → build message → publish to Kafka.

    Logs throughput every LOG_INTERVAL messages so you can watch the
    rate in real time and confirm the delay settings are working.
    """
    feature_columns = load_feature_columns(FEATURE_COLUMNS_PATH)
    producer = build_producer()

    sent = 0
    errors = 0
    start_time = time.monotonic()

    logger.info("Starting producer → topic '%s' | delay %.1f–%.1fs/msg", KAFKA_TOPIC, MIN_DELAY, MAX_DELAY)

    for row in stream_csv_rows(PAYSIM_PATH):
        txn_id = str(uuid.uuid4())
        message = map_paysim_to_message(row, feature_columns, txn_id)

        try:
            producer.send(KAFKA_TOPIC, value=message)
        except KafkaError as exc:
            errors += 1
            logger.error("Send failed for txn %s: %s (total errors: %d)", txn_id[:8], exc, errors)
            continue

        sent += 1

        if sent % LOG_INTERVAL == 0:
            elapsed = time.monotonic() - start_time
            rate = sent / elapsed if elapsed > 0 else 0.0
            logger.info("Sent %6d messages | %5.1f msg/s | errors: %d", sent, rate, errors)

        # Random delay simulates bursty-but-realistic live traffic patterns
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    # flush() blocks until all buffered messages are acknowledged by the broker
    producer.flush()
    logger.info("Producer complete. Sent: %d | Errors: %d", sent, errors)


if __name__ == "__main__":
    try:
        run_producer()
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        sys.exit(1)
    except KafkaConnectionError:
        logger.error(
            "Could not connect to Kafka at %s after all retries. "
            "Start the stack with: docker compose up -d",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user (Ctrl+C).")
        sys.exit(0)
